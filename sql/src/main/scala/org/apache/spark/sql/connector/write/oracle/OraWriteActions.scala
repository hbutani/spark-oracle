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

import oracle.spark.DataSourceKey

import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
 *
 * @param dsKey the Oracle instance
 * @param oraTable the Oracle table being written to
 * @param inputQuerySchema schema of input Spark Plan
 * @param queryId provided by [[LogicalWriteInfo]]
 * @param writeKind APPEND/UPDATE/PARTITIONEXCHANGE
 * @param srcUpdateSpec in case the action is a [[OraWriteKind.UPDATE]] this
 *                      captures the source rows to update.
 */
case class OraWriteSpec(
    dsKey: DataSourceKey,
    oraTable: OraTable,
    inputQuerySchema: StructType,
    queryId: String,
    writeKind: OraWriteKind.Value,
    srcUpdateSpec: OraSourceUpdateSpec) {

  def setDeleteFilters(predSQL: String): OraWriteSpec = {
    this.copy(writeKind = OraWriteKind.UPDATE, srcUpdateSpec = srcUpdateSpec.updateOn(predSQL))
  }

  def setTruncate: OraWriteSpec = {
    this.copy(writeKind = OraWriteKind.UPDATE, srcUpdateSpec = srcUpdateSpec.truncate)
  }

  def setDynPartitionOverwrite: OraWriteSpec = {
    assert(srcUpdateSpec.noChangesToSource)
    this.copy(writeKind = OraWriteKind.PARTITIONEXCHANGE)
  }
}

trait OraWriteActions extends Serializable {

  val writeSpec: OraWriteSpec

  lazy val tempTableName = s"${writeSpec.oraTable.name}_oraspark_${writeSpec.queryId}"

  def createTempTableDDL: String

  /**
   * Called when [[org.apache.spark.sql.connector.write.BatchWrite.createBatchWriterFactory]]
   * is invoked.
   */
  def createTempTable: Unit = {
    // TODO
    // create the table using the dsKey
  }

  /**
   * The DDL to drop the table for this job.
   * @return
   */
  def dropTempTableDDL: String

  /**
   * Called when ''commit'' or ''abort'' happens in
   * [[org.apache.spark.sql.connector.write.BatchWrite]]
   */
  def dropTempTable: Unit = {
    // TODO
    // create the table using the dsKey
  }

  def insertTempTableDML: String

  def insertOraTableDML: String

  /**
   * Called when ''commit'' happens in
   * [[org.apache.spark.sql.connector.write.BatchWrite]].
   * Do things like delete dest Table rows that are to be updated;
   * or gather the partitions loaded into the temp table.
   */
  def prepareForInsertIntoOraTable: Unit

  /**
   * Called when ''commit'' happens in
   * * [[org.apache.spark.sql.connector.write.BatchWrite]]
   */
  def insertIntoOraTable: Unit

  def close: Unit = {
    // TODO: release resources.
  }
}

object OraWriteActions {

  def apply(writeSpec: OraWriteSpec): OraWriteActions = writeSpec.writeKind match {
    case OraWriteKind.APPEND | OraWriteKind.UPDATE => OraNonPartitionTempTableActions(writeSpec)
    case OraWriteKind.PARTITIONEXCHANGE => OraPartitionTempTableActions(writeSpec)
  }
}

/**
 *  - a non-partitioned temp table is created; its schema is same as the destination table.
 *  - [[DataWriter]] instances write rows to this table.
 *  - On job commit
 *    - for truncate/update, destination rows are first deleted.
 *    - temp table rows are copied into destination table
 *    - temp table is dropped.
 */
case class OraNonPartitionTempTableActions(writeSpec: OraWriteSpec) extends OraWriteActions {

  override def createTempTableDDL: String = ???

  override def dropTempTableDDL: String = ???

  override def insertTempTableDML: String = ???

  override def insertOraTableDML: String = ???

  override def prepareForInsertIntoOraTable: Unit = ???

  override def insertIntoOraTable: Unit = ???
}

/**
 *  - a partitioned table is created using
 *  [[https://oracle-base.com/articles/12c/create-table-for-exchange-with-table-12cr2]]
 *  - [[DataWriter]] instances write rows to this table. Ideally input data is shuffled
 *    on partition columns, so each task writes a few partitions.
 *  - On job commit
 *    - query oracle dictionary for partitions created on temp table
 *    - replace destination table partitions for these using
 *      [[https://oracle-base.com/articles/12c/create-table-for-exchange-with-table-12cr2]]
 *    - temp table is dropped.
 */
case class OraPartitionTempTableActions(writeSpec: OraWriteSpec) extends OraWriteActions {

  override def createTempTableDDL: String = ???

  override def dropTempTableDDL: String = ???

  override def insertTempTableDML: String = ???

  override def insertOraTableDML: String = ???

  override def prepareForInsertIntoOraTable: Unit = ???

  override def insertIntoOraTable: Unit = ???
}
