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

import java.math.BigDecimal
import java.sql.{Date, Timestamp}

import oracle.spark.{ConnectionManagement, ORASQLUtils}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.oracle.OraMetadataMgrInternalTest
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, DeleteFromTableExec, OverwriteByExpressionExec, OverwritePartitionsDynamicExec, V2CommandExec, V2TableWriteExec}
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.readpath.{AbstractReadTests, ReadPathTestSetup}
import org.apache.spark.sql.oracle.testutils.DataGens.{not_null, null_percent_1, null_percent_15}
import org.apache.spark.sql.oracle.testutils.TestDataSetup
import org.apache.spark.sql.oracle.testutils.TestDataSetup._
import org.apache.spark.sql.types.StructType


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

  def scenarioQE(scenario : WriteScenario) : Seq[QueryExecution] = scenario match {
    case cs : CompositeInsertScenario => cs.steps.map(scenarioQE).flatten
    case _ => Seq(getAroundTestBugQE(scenario.sql))
  }

  def scenarioDF(scenario : WriteScenario) : Seq[DataFrame] = scenario match {
    case cs : CompositeInsertScenario => cs.steps.map(scenarioDF).flatten
    case _ => Seq(getAroundTestBugDF(scenario.sql))
  }


  def scenarioInfo(scenario : WriteScenario) : (String, String) = {

    import scala.collection.JavaConverters._

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

    try {

      if (scenario.dynPartOvwrtMode) {
        TestOracleHive.sparkSession.sqlContext.setConf(
          "spark.sql.sources.partitionOverwriteMode",
          "dynamic"
        )
      }

      val r = for (qe <- scenarioQE(scenario)) yield {

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
      }

      val (ops, plans) = r.unzip

      (ops.mkString("\n"), plans.mkString("\n"))

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

        val (qEs, steps) = (scenarioQE(scenario), scenario.steps)

        for((qe, step) <- qEs.zip(steps)) {
          println(
            s"""Logical Plan:
               |${qe.optimizedPlan}
               |Spark Plan:
               |${qe.sparkPlan}""".stripMargin
          )
          scenarioDF(step)
        }

        scenario.validateTest

      } finally {
        TestOracleHive.sparkSession.sqlContext.setConf(
          "spark.sql.sources.partitionOverwriteMode",
          "static"
        )
        scenario.testCleanUp
      }
    }
  }
  // scalastyle:on println

  private def setupWriteSrcTab: Unit = {
    try {

      // Uncomment to create the src_tab_for_writes data file
      // AbstractWriteTests.write_src_df(1000)

      TestOracleHive.sql("use spark_catalog")

      val tblExists =
        TestOracleHive.sparkSession.sessionState.catalog.tableExists(TableIdentifier(src_tab))

      if (!tblExists) {

        TestOracleHive.sql(
          s"""
             |create table ${src_tab}(
             |${AbstractWriteTests.unit_test_table_cols_ddl}
             |)
             |using parquet
             |OPTIONS (path "${AbstractWriteTests.absoluteSrcDataPath}")
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

  val seed = org.scalacheck.rng.Seed(1234567)
  val params = Gen.Parameters.default.withInitialSeed(seed)

  val src_tab = "src_tab_for_writes"
  val qual_src_tab = "spark_catalog.default.src_tab_for_writes"

  trait WriteScenario {
    val name: String
    def sql : String
    val dynPartOvwrtMode: Boolean

    def steps : Seq[WriteScenario] = Seq(this)
    def destTableSizeAfterScenario : Int
    def validateTest : Unit = ()
    def testCleanUp : Unit = ()
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

    lazy val pvals : (String, String) = state_channel_val.next()

    lazy val sql = {
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
        val sel_list_withoutPSpec = s"c_varchar2_40, c_int, state, channel "
        val sel_list_withPSpec = s"c_varchar2_40, c_int, channel "
        val pSpec = s"partition(state = '${pvals._1}')"
        val qrySelList = if (!withPartSpec) sel_list_withoutPSpec else sel_list_withPSpec
        val qryCond = if (!withPartSpec) "" else s"where state = '${pvals._1}'"
        val qry =
          s"""select ${qrySelList}
             |from ${qual_src_tab}
             |${qryCond}""".stripMargin

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

    // scalastyle:off line.size.limit
    override def toString: String =
      s"""name=${name}, table_is_part=${tableIsPart}, insert_ovrwt=${insertOvrwt}, with_part_spec=${withPartSpec}, dynami_part_mode=${dynPartOvwrtMode}
         |SQL:
         |${sql}
         |""".stripMargin


    override def destTableSizeAfterScenario : Int = if (tableIsPart && withPartSpec) {
      srcData_PartCounts(pvals._1)
    } else 1000

    // scalastyle:on line.size.limit

    /*
     * - Check that the table/partition has the write number of rows
     * - For Write with PartSpec, check all other partitions are empty.
     */
    override def validateTest: Unit = {

      val dsKey = ConnectionManagement.getDSKeyInTestEnv
      val query = if (tableIsPart) {
        if (withPartSpec) {
          s"""
            |select count(*)
            |from sparktest.unit_test_write_partitioned
            |where state = '${pvals._1}'""".stripMargin
        } else {
          "select count(*) from sparktest.unit_test_write_partitioned"
        }
      } else {
        "select count(*) from sparktest.unit_test_write"
      }

      assert(
        ORASQLUtils.performDSQuery[Int](
          dsKey,
          query,
          s"validating Insert Scenario ${id}",
        ) { rs =>
          rs.next()
          rs.getInt(1)
        } == destTableSizeAfterScenario
      )

      if (tableIsPart && withPartSpec) {
        assert(
          ORASQLUtils.performDSQuery[Int](
            dsKey,
            s"""
               |select count(*)
               |from sparktest.unit_test_write_partitioned
               |where state != '${pvals._1}'""".stripMargin,
            s"validating Insert Scenario ${id}",
          ) { rs =>
            rs.next()
            rs.getInt(1)
          } == 0
        )
      }
    }

    override def testCleanUp : Unit = {
      val dsKey = ConnectionManagement.getDSKeyInTestEnv
      val dml = if (tableIsPart) {
        "truncate table sparktest.unit_test_write_partitioned"
      } else {
        "truncate table sparktest.unit_test_write"
      }
      ORASQLUtils.performDSDML(
        dsKey,
        dml,
        s"cleaning up Insert Scenario ${id}"
      )
    }

  }

  case class DeleteScenario(id : Int, tableIsPart: Boolean) extends WriteScenario {
    val name : String = s"Delete Scenario ${id}"
    lazy val sql = {
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

    val dynPartOvwrtMode: Boolean = false

    override def toString: String = s"""name=${name}, table_is_part=${tableIsPart}"""

    override def destTableSizeAfterScenario : Int = 0
  }

  case class TruncateScenario(id : Int, tableIsPart: Boolean, withPartSpec : Boolean)
    extends WriteScenario {
    val name : String = s"Truncate Scenario ${id}"
    lazy val sql = {
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

    val dynPartOvwrtMode: Boolean = false

    override def toString: String = s"""name=${name}, table_is_part=${tableIsPart}"""

    override def destTableSizeAfterScenario : Int = 0
  }

  case class CompositeInsertScenario(name : String,
                                     tableIsPart: Boolean,
                                     dynPartOvwrtMode: Boolean,
                                     override val steps: Seq[WriteScenario],
                                     numRowsAfterTest : Seq[WriteScenario] => Int =
                                     steps => steps.map(_.destTableSizeAfterScenario).sum
                                    ) extends WriteScenario {
    override def sql: String =
      throw new UnsupportedOperationException("No sql for a CompositeInsertScenario")

    override def validateTest: Unit = {

      val dsKey = ConnectionManagement.getDSKeyInTestEnv
      val query = if (tableIsPart) {
        "select count(*) from sparktest.unit_test_write_partitioned"
      } else {
        "select count(*) from sparktest.unit_test_write"
      }

      assert(
        ORASQLUtils.performDSQuery[Int](
          dsKey,
          query,
          s"validating Composite Insert Scenario '${name}''",
        ) { rs =>
          rs.next()
          rs.getInt(1)
        } == destTableSizeAfterScenario
      )
    }

    override def testCleanUp : Unit = {
      val dsKey = ConnectionManagement.getDSKeyInTestEnv
      val dml = if (tableIsPart) {
        "truncate table sparktest.unit_test_write_partitioned"
      } else {
        "truncate table sparktest.unit_test_write"
      }
      ORASQLUtils.performDSDML(
        dsKey,
        dml,
        s"cleaning up Insert Scenario '${name}''"
      )
    }

    override def destTableSizeAfterScenario : Int = numRowsAfterTest(steps)
  }

  val scenarios : Seq[WriteScenario] = Seq(
    InsertScenario(0, false, false, false, false), // insert non-part
    InsertScenario(1, false, true, false, false),  // insert overwrite non-part
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

  val compositeScenarios = Seq(
    CompositeInsertScenario(
      "Non-Part multiple inserts",
      false,
      false,
      Seq(
        InsertScenario(0, false, false, false, false), // insert non-part
        InsertScenario(0, false, false, false, false) // insert non-part
      )
    ),
    CompositeInsertScenario(
      "Non-Part insert followed by insert overwrite",
      false,
      false,
      Seq(
        InsertScenario(0, false, false, false, false), // insert non-part
        InsertScenario(1, false, true, false, false)  // insert overwrite non-part
      ),
      _ => 1000),
    CompositeInsertScenario(
      "Multiple part inserts",
      true,
      false,
      Seq(
        InsertScenario(5, true, false, true, false),  // insert with partSpec
        InsertScenario(5, true, false, true, false),  // insert with partSpec
      )
    ),
    CompositeInsertScenario(
      "Part insert followed by insert overwrite of part",
      true,
      false,
      Seq(
        InsertScenario(5, true, false, true, false),  // insert with partSpec
        InsertScenario(8, true, true, true, false),  // insert overwrite with partSpec
      ),
      steps => {
        val pVals0 = steps(0).asInstanceOf[InsertScenario].pvals
        val pVals1 = steps(1).asInstanceOf[InsertScenario].pvals

        if (pVals0._1 == pVals1._1) {
          steps(1).destTableSizeAfterScenario
        } else {
          steps.map(_.destTableSizeAfterScenario).sum
        }
      }
    ),
    CompositeInsertScenario(
      "Part insert in dynPart mode followed by insert overwrite in dynPart mode of part",
      true,
      false,
      Seq(
        InsertScenario(5, true, false, true, false),  // insert with partSpec
        InsertScenario(9, true, true, true, true),  // insert overwrite with partSpec in dynPartMode
      ),
      steps => {
        val pVals0 = steps(0).asInstanceOf[InsertScenario].pvals
        val pVals1 = steps(1).asInstanceOf[InsertScenario].pvals

        if (pVals0._1 == pVals1._1) {
          steps(1).destTableSizeAfterScenario
        } else {
          steps.map(_.destTableSizeAfterScenario).sum
        }
      }
    )
  )

  val stateGen = oneOf("OR", "AZ", "PA", "MN", "NY", "CA", "OT")
  val channelGen = oneOf("D", "I", "U")

  lazy val state_channel_val: Iterator[(String, String)] = {

    listOfN(500, zip(stateGen, channelGen)).apply(params, seed).get.iterator
  }

  lazy val non_part_cond_values : Iterator[String] = {
    // c_byte gen
    val gen = TestDataSetup.NumberDataType(2, 0).gen.map(_.toString)
    listOfN(500, gen).apply(params, seed).get.iterator
  }

  def constructRow(colValues : IndexedSeq[IndexedSeq[(_, Boolean)]], i : Int) : GenericRow = {
    new GenericRow(
      Array(
      colValues(0)(i)._1.asInstanceOf[String],
      colValues(1)(i)._1.asInstanceOf[String],
      colValues(2)(i)._1.asInstanceOf[String],
      colValues(3)(i)._1.asInstanceOf[String],
      colValues(4)(i)._1.asInstanceOf[String],
      colValues(5)(i)._1.asInstanceOf[String],
      colValues(6)(i)._1.asInstanceOf[String],
      colValues(7)(i)._1.asInstanceOf[String],
      colValues(8)(i)._1.asInstanceOf[BigDecimal].byteValueExact(),
      colValues(9)(i)._1.asInstanceOf[BigDecimal].shortValueExact(),
      colValues(10)(i)._1.asInstanceOf[BigDecimal].intValueExact(),
      colValues(11)(i)._1.asInstanceOf[BigDecimal].longValueExact(),
      colValues(12)(i)._1.asInstanceOf[BigDecimal],
      colValues(13)(i)._1.asInstanceOf[BigDecimal],
      colValues(14)(i)._1.asInstanceOf[BigDecimal],
      colValues(15)(i)._1.asInstanceOf[Date],
      colValues(16)(i)._1.asInstanceOf[Timestamp],
      colValues(17)(i)._1.asInstanceOf[String],
      colValues(18)(i)._1.asInstanceOf[String]
      )
    )
  }

  val unit_test_table_cols_ddl =
    """
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
      | channel          string""".stripMargin

  val unit_test_table_scheme: DataScheme = IndexedSeq[DataType[(_, Boolean)]](
    CharDataType(1).withNullsGen(not_null),
    CharDataType(5).withNullsGen(null_percent_1),
    Varchar2DataType(10).withNullsGen(null_percent_1),
    Varchar2DataType(40).withNullsGen(null_percent_15),
    NCharDataType(1).withNullsGen(null_percent_1),
    NCharDataType(5).withNullsGen(not_null),
    NVarchar2DataType(10).withNullsGen(null_percent_15),
    NVarchar2DataType(40).withNullsGen(null_percent_15),
    NumberDataType(2, 0).withNullsGen(not_null),
    NumberDataType(4, 0).withNullsGen(null_percent_1),
    NumberDataType(9, 0).withNullsGen(null_percent_15),
    NumberDataType(18, 0).withNullsGen(null_percent_15),
    NumberDataType(25, 0).withNullsGen(null_percent_15),
    NumberDataType(25, 5).withNullsGen(null_percent_15),
    NumberDataType(25, 8).withNullsGen(null_percent_15),
    // FloatDataType.withNullsGen(null_percent_15),
    // DoubleDataType.withNullsGen(null_percent_15),
    DateDataType.withNullsGen(null_percent_15),
    TimestampDataType.withNullsGen(null_percent_15),
    CharDataType(2).withGen(stateGen).withNullsGen(not_null),
    CharDataType(2).withGen(channelGen).withNullsGen(not_null),
  )

  val unit_test_table_catalyst_schema = StructType.fromDDL(unit_test_table_cols_ddl)

  val srcDataPath =
    "src/test/resources/data/src_tab_for_writes"

  def absoluteSrcDataPath : String =
    new java.io.File(srcDataPath).getAbsoluteFile.toPath.toString

  def write_src_df(numRows : Int) : Unit = {
    val seed = org.scalacheck.rng.Seed.random
    val params = Gen.Parameters.default.withInitialSeed(seed)

    val colValues : IndexedSeq[IndexedSeq[(_, Boolean)]] =
      unit_test_table_scheme.map(c =>
        Gen.listOfN(numRows, c.gen).apply(params, seed).get.toIndexedSeq
      )

    val rows : Seq[GenericRow] = (0 until numRows) map { i =>
      constructRow(colValues, i)
    }

    val rowsRDD : RDD[Row] = TestOracleHive.sparkContext.parallelize(rows, 1)

    TestOracleHive.
      createDataFrame(rowsRDD, unit_test_table_catalyst_schema).
      write.parquet(srcDataPath)
  }

  val srcData_PartCounts = Map(
    "MN" -> 134,
    "AZ" -> 136,
    "PA" -> 136,
    "OT" -> 164,
    "OR" -> 144,
    "NY" -> 143,
    "CA" -> 143
  )

}
