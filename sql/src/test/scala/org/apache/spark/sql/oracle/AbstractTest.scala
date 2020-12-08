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

package org.apache.spark.sql.oracle

import java.io.{File, PrintStream}

import org.scalatest.{fixture, BeforeAndAfterAll}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.types.DataType

/**
 * Please read these notes when running or adding new tests.
 *
 * - In steady state tests are run against the 'mammoth' ADW instance.
 *   - since this is a cloud instance making connections and executing queries can
 *     takes 10s of seconds.
 *   - so we have optimized for this by having a checked-in 'metadata_cache' and a
 *     'resultset_cache'
 *   - but you can run into errors because of cache inconsistencies that you
 *     need to be mindful off.
 *
 * Locally you can run tests against other instances. Just configure
 * '-Dspark.oracle.test.db_instance' approriately. To run against the
 * 'mammoth' instance set: '-Dspark.oracle.test.db_instance=mammoth_medium -Dspark.oracle.test.db_wallet_loc=/Users/hbutani/oracle/wallet_mammoth'
 *
 * Never commit changes to the 'sql/src/test/resources/metadata_cache' folder
 * except when explicitly changing the metadata cache.
 * Follow this procedure below for that.
 *
 * Procedure to change the 'metadata_cache'
 * - delete 'sql/src/test/resources/metadata_cache/000005.sst',
 *   'sql/src/test/resources/metadata_cache/MANIFEST-000004'
 * - run with '.set("spark.sql.catalog.oracle.use_metadata_cache_only", "false")'
 *   in [[TestOracleHive]]
 * - run [[org.apache.spark.sql.connector.catalog.oracle.LoadMetadataCache]] only to
 *   create new cache.
 * - you may have to run it twice for 'levelDB' to comapct the log file.
 *
 * 'resultset_cache' quirk:
 * - this uses the built-in SAXXMLParser to read and write resultsets
 *   See details in [[org.apache.spark.sql.oracle.experiments.ResultSetCaching]]
 *   on setup.
 * - When on vpn the SAXParser takes 10s of seconds to load/write xml files
 *   because it cannot load the xsd file.
 * - So any steady state tun tests outside of vpn.
 *
 * Procedure to add to 'resultset_cache'
 * - the straight-forward way is to be on vpn, run the test; this will create the
 *   new xml file.
 * - a shortcut is to use a local db with the same schema and tables. Connect
 *   and run tests against it. Then change the name of the new resultset cml files
 *   so that the initial section matches mammoth dskey(-1641891193)
 *
 * You can use [[DataGens]] and [[TestDataSetup]] to create new test tables and data.
 */
abstract class AbstractTest
    extends fixture.FunSuite
    with fixture.TestDataFixture
    with BeforeAndAfterAll
    with Logging {

  // scalastyle:off println

  override def beforeAll(): Unit = {
    println("*** Starting TestCase " ++ this.toString())
    //    System.setProperty("user.timezone", "UTC")
    //    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    //    DateTimeZone.setDefault(DateTimeZone.forID("UTC"))

    new SparkSessionExtensions()(TestOracleHive.sparkSession.extensions)

    /*
     * Try to configure using log4j.properties first.
     */
    // TestOracleHive.sparkContext.setLogLevel("ERROR")
    TestOracleHive.setConf("spark.sql.files.openCostInBytes", (128 * 1024 * 1024).toString)
    TestOracleHive.setConf("spark.sql.files.maxPartitionBytes", (16 * 1024 * 1024).toString)

    TestOracleHive.sql("use oracle")

  }

  override def afterAll(): Unit = {
    TestOracleHive.sql("use tpcds")
  }

  def result(df: DataFrame): Array[Row] = {
    df.collect()
  }

  def doSQL(nm: String,
            sqlArg : Either[String, DataFrame],
            showPlan: Boolean = false,
            showResults: Boolean = false,
           numRows : Int = 20) : Unit = {
    try {
      val df = sqlArg match {
        case Left(sql) => sqlAndLog(nm, sql)
        case Right(df) => df
      }
      if (showPlan) {
        logPlan(nm, df)
      }
      if (showResults) {
        df.show(numRows, false)
      }
    } finally {}
  }

  def test(
      nm: String,
      sql: String,
      showPlan: Boolean = false,
      showResults: Boolean = false,
      numRows : Int = 20,
      setupSQL: Option[(String, String)] = None): Unit = {
    test(nm) { td =>
      println("*** *** Running Test " ++ nm)

      try {

        for ((s, e) <- setupSQL) {
          TestOracleHive.sql(s)
        }

        doSQL(nm, Left(sql), showPlan, showResults, numRows)

      } finally {
        for ((s, e) <- setupSQL) {
          TestOracleHive.sql(e)
        }
      }
    }
  }

  def cTest(
      nm: String,
      dsql: String,
      bsql: String,
      devAllowedInAproxNumeric: Double = 0.0): Unit = {
    test(nm) { td =>
      println("*** *** Running Correctness Test " ++ nm)

      try {
        val df1 = sqlAndLog(nm, dsql)
        val df2 = sqlAndLog(nm, bsql)
        assert(isTwoDataFrameEqual(df1, df2, devAllowedInAproxNumeric))
      } finally {}
    }
  }

  def sqlAndLog(nm: String, sqlStr: String): DataFrame = {
    logDebug(s"\n$nm SQL:\n" + sqlStr)
    TestOracleHive.sql(sqlStr)
  }

  def logPlan(nm: String, df: DataFrame): Unit = {
    logInfo(s"\n$nm Plan:")
    logInfo(s"\nLogical Plan:\n" + df.queryExecution.logical.toString)
    logInfo(s"\nOptimized Plan:\n" + df.queryExecution.optimizedPlan.toString)
    logInfo(s"\nPhysical Plan:\n" + df.queryExecution.sparkPlan.toString)
  }

  def roundValue(chooseRounding: Boolean, v: Any, dt: DataType): Any = {
    if (chooseRounding && v != null &&
        OraSparkUtils.isNumeric(dt) &&
        !Set[Any](
          Double.PositiveInfinity,
          Double.NegativeInfinity,
          Float.PositiveInfinity,
          Float.NegativeInfinity).contains(v)) {
      BigDecimal(v.toString).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
    } else if (v == Float.PositiveInfinity) {
      Double.PositiveInfinity
    } else if (v == Float.NegativeInfinity) {
      Double.NegativeInfinity
    } else {
      v
    }
  }

  def isTwoDataFrameEqual(
      df1: DataFrame,
      df2: DataFrame,
      devAllowedInAproxNumeric: Double,
      Sorted: Boolean = false,
      chooseRounding: Boolean = true,
      out : PrintStream = System.out): Boolean = {

    if (df1.schema != df2.schema) {
      logWarning(s"""
           |different schemas issue:
           | df1 schema = ${df1.schema}
           | df2.schema = ${df2.schema}
         """.stripMargin)
      // return false
    }

    import collection.JavaConverters._

    var df11 = df1
    var df21 = df2

    var df1_ilist = df11.queryExecution.executedPlan.executeCollect()
    var df2_ilist = df21.queryExecution.executedPlan.executeCollect()

    if (!Sorted && df1_ilist.size > 1) {

      val sortCols = df11.columns

      df1_ilist = {
        df11 = OraSparkUtils.dataFrame(
          LocalRelation(df11.queryExecution.optimizedPlan.output, df1_ilist))(
          TestOracleHive.sparkSession.sqlContext)
        df11 =
          df11.sort(sortCols.head, sortCols.tail: _*).select(sortCols.head, sortCols.tail: _*)
        df11.queryExecution.executedPlan.executeCollect()
      }

      df2_ilist = {
        df21 = OraSparkUtils.dataFrame(
          LocalRelation(df21.queryExecution.optimizedPlan.output, df2_ilist))(
          TestOracleHive.sparkSession.sqlContext)
        df21 =
          df21.sort(sortCols.head, sortCols.tail: _*).select(sortCols.head, sortCols.tail: _*)
        df21.queryExecution.executedPlan.executeCollect()
      }
    }

    var diffFound : Boolean = false

    val df1_count = df1_ilist.size
    val df2_count = df2_ilist.size
    if (df1_count != df2_count) {
      out.println(df1_count + "\t" + df2_count)
      out.println("The row count is not equal")
      // out.println(s"""df1=\n${df1_ilist.mkString("\n")}\ndf2=\n ${df2_ilist.mkString("\n")}""")
      diffFound = true
    }

    for (i <- 0 to df1_count.toInt - 1) {
      for (j <- 0 to df1.columns.size - 1) {
        val res1 = roundValue(
          chooseRounding,
          df1_ilist(i).get(j, df11.schema(j).dataType),
          df11.schema(j).dataType)
        val res2 = roundValue(
          chooseRounding,
          df2_ilist(i).get(j, df21.schema(j).dataType),
          df21.schema(j).dataType)
        // account for difference in null aggregation of javascript
        if (res2 == null && res1 != null) {
          if (!Set[Any](
                Int.MaxValue,
                Int.MinValue,
                Long.MaxValue,
                Long.MinValue,
                Double.PositiveInfinity,
                Double.NegativeInfinity,
                Float.PositiveInfinity,
                Float.NegativeInfinity,
                0).contains(res1)) {
            out.println(s"values in row $i, column $j don't match: ${res1} != ${res2}")
            diffFound = true
          }
        } else if ((OraSparkUtils.isApproximateNumeric(df1.schema(j).dataType) &&
                   (Math.abs(res1.asInstanceOf[Double] - res2.asInstanceOf[Double]) >
                     devAllowedInAproxNumeric)) ||
                   (!OraSparkUtils.isApproximateNumeric(df1.schema(j).dataType) && res1 != res2)) {
          out.println(s"values in row $i, column $j don't match: ${res1} != ${res2}")
          diffFound = true
        }
      }
    }
    logDebug("The two dataframe is equal " + df1_count)
    // println(df1_list.mkString("", "\n", ""))

    if (!diffFound && false) {
      println(s"""df1=\n${df1_ilist.mkString("\n")}\ndf2=\n ${df2_ilist.mkString("\n")}""")
    }

    diffFound
  }

  def delete(f: File): Unit = {
    if (f.exists()) {
      if (f.isDirectory) {
        f.listFiles().foreach(delete(_))
        f.delete()
      } else {
        f.delete()
      }
    }
  }

}
