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

package org.apache.spark.sql.oracle.writepath

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.oracle.OraMetadataMgrInternalTest
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, DeleteFromTableExec, OverwriteByExpressionExec, OverwritePartitionsDynamicExec, V2CommandExec, V2TableWriteExec}
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.readpath.{AbstractReadTests, ReadPathTestSetup}
import org.apache.spark.sql.oracle.testutils.TestDataSetup

abstract class AbstractWriteTests extends AbstractReadTests with OraMetadataMgrInternalTest {

  import AbstractWriteTests._

  /**
   * The test inserts involve reading from a spark_catalog.default namespace table
   * and writing to oracle.sparktest namespace. So insert statement has the form:
   * {{{insert into unit_test_write ... select .. from spark_catalog.default.src_tab_for_writes}}}
   *
   * But using the qualified name `spark_catalog.default.src_tab_for_writes` throws an exception
   * because [[TestHiveQueryExecution.analyzed]] invokes
   * [[MultipartIdentifierHelper.asTableIdentifier]] when collecting
   * [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation]] from the plan.
   * This causes an exception to be thrown, as MultipartIdentifierHelper only
   * allows names with at most 2 parts.
   *
   * To get around this, we construct a [[QueryExecution]], bypassing
   * [[TestHiveQueryExecution.analyzed]] logic.
   *
   * @param sql
   * @return
   */
  def getAroundTestBugQE(sql: String): QueryExecution = {
    val plan = TestOracleHive.sparkSession.sessionState.sqlParser.parsePlan(sql)
    new QueryExecution(TestOracleHive.sparkSession, plan)
  }

  def getAroundTestBugDF(sql: String): DataFrame = {
    val qE = getAroundTestBugQE(sql)
    Dataset.ofRows(TestOracleHive.sparkSession, qE.analyzed)
  }

  def scenarioQE(scenario : WriteScenario) : QueryExecution = getAroundTestBugQE(scenario.sql)

  def scenarioDF(scenario : WriteScenario) : DataFrame = getAroundTestBugDF(scenario.sql)


  def scenarioInfo(scenario : WriteScenario) : (String, String) = {

    try {

      if (scenario.dynPartOvwrtMode) {
        TestOracleHive.sparkSession.sqlContext.setConf(
          "spark.sql.sources.partitionOverwriteMode",
          "dynamic"
        )
      }

      val qe = scenarioQE(scenario)
      (s"""Scenario: ${scenario}
         |Write Operation details: ${writeOpInfo(qe.sparkPlan)}
         |""".stripMargin,
        s"""Scenario: ${scenario.name}
           |Logical Plan:
           |${qe.optimizedPlan}
           |Spark Plan:
           |${qe.sparkPlan}
           |""".stripMargin
        )

    } finally {
      TestOracleHive.sparkSession.sqlContext.setConf(
        "spark.sql.sources.partitionOverwriteMode",
        "static"
      )
    }


  }

  // scalastyle:off println
  def performTest(scenario : WriteScenario) : Unit = {
    test(scenario.name) {td =>
      println(s"Performing Write Scenario: ${scenario}")

      try {

        if (scenario.dynPartOvwrtMode) {
          TestOracleHive.sparkSession.sqlContext.setConf(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic"
          )
        }

        val qe = scenarioQE(scenario)
        println(
          s"""Logical Plan:
             |${qe.optimizedPlan}
             |Spark Plan:
             |${qe.sparkPlan}""".stripMargin
        )

        scenarioDF(scenario)

      } finally {
        TestOracleHive.sparkSession.sqlContext.setConf(
          "spark.sql.sources.partitionOverwriteMode",
          "static"
        )
      }
    }
  }
  // scalastyle:on println

  private def setupWriteSrcTab: Unit = {
    try {
      TestOracleHive.sql("use spark_catalog")

      val tblExists =
        TestOracleHive.sparkSession.sessionState.catalog.tableExists(TableIdentifier(src_tab))

      if (!tblExists) {

        TestOracleHive.sql(
          s"""
             |create table ${src_tab}(
             |C_CHAR_1         string      ,
             |C_CHAR_5         string      ,
             |C_VARCHAR2_10    string      ,
             |C_VARCHAR2_40    string      ,
             |C_NCHAR_1        string      ,
             |C_NCHAR_5        string      ,
             |C_NVARCHAR2_10   string      ,
             |C_NVARCHAR2_40   string      ,
             |C_BYTE           tinyint     ,
             |C_SHORT          smallint    ,
             |C_INT            int         ,
             |C_LONG           bigint      ,
             |C_NUMBER         decimal(25,0),
             |C_DECIMAL_SCALE_5 decimal(25,5),
             |C_DECIMAL_SCALE_8 decimal(25,8),
             |C_DATE           date        ,
             |C_TIMESTAMP      timestamp,
             |state            string,
             | channel          string
             |) using parquet
             |""".stripMargin).show()


      }
    } finally {
      TestOracleHive.sql("use oracle.sparktest")
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    setupWriteSrcTab
  }
}

object AbstractWriteTests {

  import org.scalacheck.Gen
  import org.scalacheck.Gen._
  import scala.collection.JavaConverters._

  val seed = org.scalacheck.rng.Seed.random
  val params = Gen.Parameters.default.withInitialSeed(seed)

  val src_tab = "src_tab_for_writes"
  val qual_src_tab = "spark_catalog.default.src_tab_for_writes"

  trait WriteScenario {
    val name: String
    val sql : String
    val dynPartOvwrtMode: Boolean
  }

  case class InsertScenario(
      id: Int,
      tableIsPart: Boolean,
      insertOvrwt: Boolean,
      withPartSpec: Boolean,
      dynPartOvwrtMode: Boolean)  extends WriteScenario {

    // tableIsPart => !withPartSpec && !dynPartOvwrtMode
    assert((!tableIsPart && (!withPartSpec && !dynPartOvwrtMode)) || tableIsPart)

    // !insertOvrwt => !dynPartOvwrtMode
    assert((!insertOvrwt && !dynPartOvwrtMode) || insertOvrwt)

    val name : String = s"Insert Scenario ${id}"

    lazy val sql = insertStat(tableIsPart, insertOvrwt, withPartSpec)

    // scalastyle:off line.size.limit
    override def toString: String =
      s"""name=${name}, table_is_part=${tableIsPart}, insert_ovrwt=${insertOvrwt}, with_part_spec=${withPartSpec}, dynami_part_mode=${dynPartOvwrtMode}
         |SQL:
         |${sql}
         |""".stripMargin

    // scalastyle:on line.size.limit

  }

  case class DeleteScenario(id : Int, tableIsPart: Boolean) extends WriteScenario {
    val name : String = s"Delete Scenario ${id}"
    lazy val sql = deleteStat(tableIsPart)
    val dynPartOvwrtMode: Boolean = false

    override def toString: String = s"""name=${name}, table_is_part=${tableIsPart}"""
  }

  case class TruncateScenario(id : Int, tableIsPart: Boolean, withPartSpec : Boolean)
    extends WriteScenario {
    val name : String = s"Truncate Scenario ${id}"
    lazy val sql = truncateStat(tableIsPart, withPartSpec)
    val dynPartOvwrtMode: Boolean = false

    override def toString: String = s"""name=${name}, table_is_part=${tableIsPart}"""
  }

  val scenarios : Seq[WriteScenario] = Seq(
    InsertScenario(0, false, false, false, false),
    InsertScenario(1, false, true, false, false),
    DeleteScenario(2, false),
    // TRUNCATE TABLE is not supported for v2 tables.
    // TruncateScenario(3, false, false),

    // partition table inserts
    InsertScenario(4, true, false, false, false), // insert
    InsertScenario(5, true, false, true, false),  // insert with partSpec
    InsertScenario(6, true, true, false, false),  // insert overwrite
    InsertScenario(7, true, true, false, true),   // insert overwrite in dynPartMode
    InsertScenario(8, true, true, true, false),  // insert overwrite with partSpec
    InsertScenario(9, true, true, true, true),  // insert overwrite with partSpec in dynPartMode
    DeleteScenario(10, true),
    // TRUNCATE TABLE is not supported for v2 tables.
    // TruncateScenario(11, true, false),
    // TruncateScenario(12, true, true)
  )

  def appendDetails(op: AppendDataExec): String = {
    s"""
       |Append Operation:
       |  Destination table = ${op.table.name()}
       |  WriteOptions = ${op.writeOptions.asScala.mkString(",")}
       |Input query plan:
       |${op.query.treeString}
       |""".stripMargin
  }

  def overWrtByExprDetails(op: OverwriteByExpressionExec): String = {
    s"""
       |OverwriteByExpression Operation:
       |  Destination table = ${op.table.name()}
       |  Delete Filters = ${op.deleteWhere.mkString(", ")}
       |  WriteOptions = ${op.writeOptions.asScala.mkString(",")}
       |Input query plan:
       |${op.query.treeString}
       |""".stripMargin
  }

  def overWrtPartDynDetails(op: OverwritePartitionsDynamicExec): String = {
    s"""
       |OverwritePartitionsDynamic Operation:
       |  Destination table = ${op.table.name()}
       |  WriteOptions = ${op.writeOptions.asScala.mkString(",")}
       |Input query plan:
       |${op.query.treeString}
       |""".stripMargin
  }

  def deleteDetails(op : DeleteFromTableExec) : String = {
    s"""
       |DeleteFromTable Operation:
       |  Destination table = ${op.table.asInstanceOf[Table].name()}
       |  condition = ${op.condition.mkString(", ")}
       |  """.stripMargin
  }

  def writeOpInfo(sparkPlan : SparkPlan) : String = {
    val writeOps = sparkPlan find {
      case op : V2CommandExec => true
      case _ => false
    }

    if (writeOps.size == 0) {
      "No write operation in Spark Plan"
    } else if (writeOps.size > 1) {
      "multiple write operations in Spark Plan"
    } else {
      val writeOp = writeOps.head
      writeOp match {
        case op: AppendDataExec => appendDetails(op)
        case op: OverwriteByExpressionExec => overWrtByExprDetails(op)
        case op: OverwritePartitionsDynamicExec => overWrtPartDynDetails(op)
        case op : DeleteFromTableExec => deleteDetails(op)
        case _ => s"Unknown Operator Type: ${writeOp.getClass.getName}"
      }
    }
  }

  lazy val state_channel_val: Iterator[(String, String)] = {
    val stateGen = oneOf("OR", "AZ", "PA", "MN", "NY", "CA", "OT")
    val channelGen = oneOf("D", "I", "U")
    listOfN(500, zip(stateGen, channelGen)).apply(params, seed).get.iterator
  }

  lazy val non_part_cond_values : Iterator[String] = {
    // c_byte gen
    val gen = TestDataSetup.NumberDataType(2, 0).gen.map(_.toString)
    listOfN(500, gen).apply(params, seed).get.iterator
  }

  def insertStat(tableIsPart: Boolean, insertOvrwt: Boolean, withPartSpec: Boolean): String = {
    if (!tableIsPart) {
      val dest_tab = "unit_test_write"
      val sel_list = s"""C_CHAR_1, C_CHAR_5, C_VARCHAR2_10, C_VARCHAR2_40, C_NCHAR_1, C_NCHAR_5,
                        |C_NVARCHAR2_10, C_NVARCHAR2_40, C_BYTE, C_SHORT, C_INT, C_LONG, C_NUMBER,
                        |C_DECIMAL_SCALE_5, C_DECIMAL_SCALE_8, C_DATE, C_TIMESTAMP""".stripMargin
      val qry =
        s"""select ${sel_list}
           |from ${qual_src_tab}""".stripMargin
      val insClausePrefix = if (insertOvrwt) {
        s"insert overwrite table ${dest_tab}"
      } else {
        s"insert into ${dest_tab}"
      }

      s"""${insClausePrefix}
         |${qry}""".stripMargin

    } else {
      val dest_tab = "unit_test_write_partitioned"
      val pvals = state_channel_val.next()
      val sel_list_withoutPSpec = s"c_varchar2_40, c_int, state, channel "
      val sel_list_withPSpec = s"c_varchar2_40, c_int, channel "
      val pSpec = s"partition(state = '${pvals._1}')"
      val qrySelList = if (!withPartSpec) sel_list_withoutPSpec else sel_list_withPSpec
      val qry =
        s"""select ${qrySelList}
           |from ${qual_src_tab}""".stripMargin

      val partClause = if (withPartSpec) pSpec else ""
      val insClausePrefix = if (insertOvrwt) {
        s"insert overwrite table ${dest_tab}"
      } else {
        s"insert into ${dest_tab}"
      }

      s"""${insClausePrefix} ${partClause}
         |${qry}""".stripMargin
    }
  }

  def deleteStat(tableIsPart: Boolean): String = {
    if (!tableIsPart) {
      val cond = s"c_byte = ${non_part_cond_values.next()}"
      s"""delete from unit_test_write
         |where ${cond}""".stripMargin
    } else {
      val pvals = state_channel_val.next()
      val pCond = s"state = '${pvals._1}' and channel = '${pvals._2}'"
      s"""delete from unit_test_write_partitioned
         |where ${pCond}""".stripMargin
    }
  }

  def truncateStat(tableIsPart: Boolean, withPartSpec : Boolean): String = {
    if (!tableIsPart) {
      s"""truncate table unit_test_write""".stripMargin
    } else {
      val pSpec = if (withPartSpec) {
        val pvals = state_channel_val.next()
        s"partition(state = '${pvals._1}', channel = '${pvals._2}')"
      } else ""
      s"""truncate table unit_test_write_partitioned ${pSpec}""".stripMargin
    }
  }

}
