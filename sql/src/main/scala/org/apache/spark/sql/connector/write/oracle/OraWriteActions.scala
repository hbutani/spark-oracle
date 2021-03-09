/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.write.oracle

import oracle.spark.{DataSourceKey, ORASQLUtils}

import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.oracle.{OraSparkConfig, SQLSnippet}
import org.apache.spark.sql.oracle.expressions.OraExpression
import org.apache.spark.sql.types.StructType

/**
 *
 * @param dsKey the Oracle instance
 * @param oraTable the Oracle table being written to
 * @param inputQuerySchema schema of input Spark Plan
 * @param queryId provided by [[LogicalWriteInfo]]
 * @param isDynamicPartition is operation in Dynamic partitionOverwriteMode mode
 * @param deleteCond predicate to apply to delete existing rows/partitions
 * @param isTruncate truncate exisitng rows/partitions
 */
case class OraWriteSpec(
    dsKey: DataSourceKey,
    oraTable: OraTable,
    inputQuerySchema: StructType,
    queryId: String,
    isDynamicPartitionMode : Boolean = false,
    deleteCond: Option[OraExpression] = None,
    isTruncate: Boolean = false) {

  def setDeleteFilters(_deleteCond: OraExpression): OraWriteSpec = {
    this.copy(deleteCond = Some(_deleteCond))
  }

  def setTruncate: OraWriteSpec = {
    this.copy(isTruncate = true)
  }

  def setDynPartitionOverwriteMode : OraWriteSpec = {
    assert(!deleteCond.isDefined && !isTruncate)
    this.copy(isDynamicPartitionMode = true)
  }
}

/**
 * Represents a technique for transactionally updating a destination oracle table based
 * on actions in Spark tasks. Its responsiblity is broken down into:
 * - createTempTable
 * - provide the DML statement to write to the temp table. This is used to
 *   setup [[OraInsertStatement]] and write rows in Spark Tasks.
 * - prepareForUpdate, this is called before the updateDestTable call.
 *   here the technique can gather metadata like partitions written.
 * - updateDestTable
 */
trait OraWriteActions extends Serializable {

  val writeSpec: OraWriteSpec

  lazy val tempTableName = {
    val s = s"oraspark_${writeSpec.queryId}_${writeSpec.oraTable.name}"
    val s1 = if ( s.length > 30) {
      s.substring(0, 30)
    } else s

    s""""${s1}""""
  }

  lazy val dest_tab_name = s"${writeSpec.oraTable.schema}.${writeSpec.oraTable.name}"


  def createTempTableDDL: String

  def insertTempTableDML: String

  def deleteOraTableDML : Option[String]

  def insertOraTableDML: String

  /**
   * The DDL to drop the table for this job.
   * @return
   */
  def dropTempTableDDL: String =
    s"drop table ${tempTableName} PURGE"

  /**
   * Called when [[org.apache.spark.sql.connector.write.BatchWrite.createBatchWriterFactory]]
   * is invoked.
   */
  def createTempTable: Unit = {
    ORASQLUtils.performDSDDL(writeSpec.dsKey,
      createTempTableDDL,
      s"Creating Temp Table ${tempTableName} for insert into ${dest_tab_name}")
  }

  /**
   * Called when ''commit'' happens in
   * [[org.apache.spark.sql.connector.write.BatchWrite]].
   * Do things like gather the partitions loaded into the temp table.
   */
  def prepareForUpdate: Unit


  /**
   * Called when [[OraBatchWrite]] is asked to commit the job.
   */
  def updateDestTable : Unit

  /**
   * Called when ''commit'' or ''abort'' happens in
   * [[org.apache.spark.sql.connector.write.BatchWrite]]
   */
  def dropTempTable: Unit = {
    ORASQLUtils.performDSDDL(writeSpec.dsKey,
      dropTempTableDDL,
      s"Dropping Temp Table ${tempTableName}, setup for insert into ${dest_tab_name}")
  }

  def close: Unit = ()
}

object OraWriteActions {

  def apply(writeSpec: OraWriteSpec): OraWriteActions = BasicWriteActions(writeSpec)
}

/**
 * - handles all scenarios
 * - no special processing for inserting into temp table
 *   - creates a nologging non-partitioned temp table
 *   - tasks write to it.
 * - no special processing for deleting existing rows
 *   - issues a delete
 * - no special processing foe inserting new rows
 *   - issues a insert with select * from temp_table
 *   - select query block has PARALLEL hint on temp_table
 *   - hint on INSERT can be specified by [[OraSparkConfig.INSERT_INTO_DEST_TABLE_STAT_HINTS]]
 *
 * @param writeSpec
 */
case class BasicWriteActions(writeSpec: OraWriteSpec) extends OraWriteActions {
  def createTempTableDDL: String = {
    val tableSpace = OraSparkConfig.getConf(OraSparkConfig.TABLESPACE_FOR_TEMP_TABLES)

    val tableSpaceClause = if (tableSpace.isDefined) {
      s"tablespace ${tableSpace.get}"
    } else ""

    s"""create table ${tempTableName}
       |${tableSpaceClause} nologging
       |as select * from ${dest_tab_name} where 1=2""".stripMargin
  }

  def insertTempTableDML: String = {
    val colList = writeSpec.oraTable.columns.map(_.name).mkString(", ")
    val bindList = Seq.fill(writeSpec.oraTable.columns.size)("?").mkString(", ")

    s"""insert /*+ APPEND */ into ${tempTableName}
       |( ${colList})
       |values (${bindList})""".stripMargin
  }

  /**
   * - for dynamicPartitionMode delete partitions for which a row exists in the TempTable
   * - if a `deleteCond` is specified, delete rows based on the partSpec.
   * @return
   */
  def deleteOraTableDML : Option[String] = {
    import org.apache.spark.sql.oracle.OraSQLImplicits._

    val destTab = SQLSnippet.tableQualId(writeSpec.oraTable)
    val delClause = osql"delete from ${destTab}"

    if (writeSpec.isDynamicPartitionMode) {
      val partCols = writeSpec.oraTable.partitionSchema.fields.map(_.name)
      val partColList = partCols.mkString(", ")
      val dynamicPartListClause = SQLSnippet.literalSnippet(
        s"(${partColList}) in (select distinct ${partColList} from ${tempTableName})"
      )

      Some(osql"${delClause} where ${dynamicPartListClause}".sql)
    } else if (writeSpec.deleteCond.isDefined) {
      val delCond = osql"${writeSpec.deleteCond.get.reifyLiterals}"
      Some(osql"${delClause} where ${delCond}".sql)
    } else if (writeSpec.isTruncate) {
      Some(osql"${delClause}".sql)
    } else None
  }

  def insertOraTableDML: String = {
    val insertHints = OraSparkConfig.getConf(OraSparkConfig.INSERT_INTO_DEST_TABLE_STAT_HINTS)
    val insertHintsClause = if (insertHints.isDefined) {
      s"/*+ ${insertHints.get} */"
    } else ""

    s"""insert ${insertHintsClause}
       |into ${dest_tab_name}
       |select /*+ PARALLEL(${tempTableName}) */ * from ${tempTableName}""".stripMargin
  }

  def prepareForUpdate: Unit = ()

  def updateDestTable : Unit = {

    ORASQLUtils.performDSSQLsInTransaction(writeSpec.dsKey,
      deleteOraTableDML.toSeq :+ insertOraTableDML,
      s"Update Destination Table ${dest_tab_name} based on rows in ${tempTableName}")
  }
}


/**
 * TODO
 * Oracle List Partitioned Tables match the semantics of Spark Partitioning.
 * In this case of `insert overwrite` we can:
 * - drop partitions based on `partition spec` in insert dml
 * - exchange partitions from temp table.
 *
 * @param writeSpec
 */
abstract class ListPartitionedWriteAction(val writeSpec: OraWriteSpec) extends OraWriteActions
