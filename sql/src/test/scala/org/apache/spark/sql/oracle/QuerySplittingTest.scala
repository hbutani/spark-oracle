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

import java.io.{ByteArrayOutputStream, PrintStream}

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.tpcds.TPCDSQueryMap

// scalastyle:off println
/**
 * 1. Run with `spark.sql.catalog.oracle.use_resultset_cache=false` in [[TestOracleHive]]
 *    Setting it here doesn't get applied. [[ResultSetCache]] gets initialized once.
 */
abstract class QuerySplittingTest extends AbstractTest
  with PlanTestHelpers with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    super.beforeAll()
    setupSplitting(true, split_100k)
    TestOracleHive.sql("set spark.sql.catalog.oracle.use_resultset_cache=false")
    TestOracleHive.sql("set spark.sql.oracle.allow.splitresultset=true")
  }

  override def afterAll(): Unit = {
    TestOracleHive.sql("set spark.sql.catalog.oracle.use_resultset_cache=true")
    TestOracleHive.sql("set spark.sql.oracle.allow.splitresultset=false")
    setupSplitting(false, split_1m)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    setupSplitting(true, split_100k)
  }

  val split_1k = "1kb"
  val split_10k = "10kb"
  val split_100k = "100kb"
  val split_1m = "1Mb"

  def setupSplitting(implicit qSplit : Boolean, splitTarget : String) : Unit = {
    TestOracleHive.sql(s"set spark.sql.oracle.enable.querysplitting=${qSplit}")
    TestOracleHive.sql(s"set spark.sql.oracle.querysplit.target=${splitTarget}")
  }

  def collect(sql : String)(implicit qSplit : Boolean, splitTarget : String) : DataFrame = {
    setupSplitting
    val df = TestOracleHive.sql(sql)
    val df_ilist = df.queryExecution.executedPlan.executeCollect()

    OraSparkUtils.dataFrame(LocalRelation(df.queryExecution.optimizedPlan.output, df_ilist))(
      TestOracleHive.sparkSession.sqlContext)
  }

  private def testSplitting(sql : String,
                            splitLevel : String) : Unit = {
    val df1 = collect(sql)(true, splitLevel)
    val df2 = collect(sql)(false, split_100k)
    val bs = new ByteArrayOutputStream()
    val out : PrintStream = new PrintStream(bs)

    if (!isTwoDataFrameEqual(df1, df2, 0.0, false, true, out)) {
      println("Error:")
      println(new String(bs.toByteArray))
    }
  }

  test("partitionFilterSplit") { td =>
    testSplitting(
      """
        |select ss_item_sk, ss_ext_sales_price
        |from store_sales
        |where SS_SALES_PRICE > 100 and ss_sold_date_sk > 2451000""".stripMargin,
      split_100k
    )
  }


  test("partitionSplit") { td =>
    testSplitting(
      """
        |select ss_item_sk, ss_ext_sales_price
        |from store_sales
        |where SS_SALES_PRICE > 100""".stripMargin,
      split_1m
    )
  }

  test("rowIdSplit") { td =>

    testSplitting(
      """
        |select C_CUSTOMER_SK, C_FIRST_NAME
        |from customer
        |where C_BIRTH_MONTH = 1
        |""".stripMargin,
      split_10k
    )
  }

  test("innerJoinRowIdSplit") { td =>
    testSplitting(
      """
        |select ss_item_sk, ss_ext_sales_price, C_CUSTOMER_SK, C_FIRST_NAME
        |from store_sales join customer on c_customer_sk = ss_customer_sk
        |where SS_SALES_PRICE > 100""".stripMargin,
      split_1m
    )
  }

  test("outerJoinResultSetSplit") { td =>
    testSplitting(
      """
        |select ss_item_sk, ss_ext_sales_price, C_CUSTOMER_SK, C_FIRST_NAME
        |from store_sales left outer join customer on c_current_addr_sk = ss_customer_sk
        |where SS_SALES_PRICE > 100""".stripMargin,
      split_1m
    )
  }

  test("outerJoinResultSetSplit2") { td =>
    testSplitting(
      """
        |select C_CURRENT_ADDR_SK, ca_address_sk, C_FIRST_NAME
        |from customer left outer join customer_address on C_CURRENT_ADDR_SK = ca_address_sk
        |where c_birth_day = 1""".stripMargin,
      split_10k
    )
  }

  test("q98") { td =>
    testSplitting(
      TPCDSQueryMap.q98,
      split_1m
    )
  }

  test("q2") { td =>
    testSplitting(
      TPCDSQueryMap.q2,
      split_1k
    )
  }

  test("q71") { td =>
    testSplitting(
      TPCDSQueryMap.q71,
      split_1k
    )
  }

  test("q34") { td =>
    testSplitting(
      TPCDSQueryMap.q34,
      split_1k
    )
  }

  test("q99") { td =>
    testSplitting(
      TPCDSQueryMap.q99,
      split_1k
    )
  }
}
