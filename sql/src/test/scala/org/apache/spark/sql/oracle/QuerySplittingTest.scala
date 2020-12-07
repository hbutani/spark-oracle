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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.hive.test.oracle.TestOracleHive

// scalastyle:off println
/**
 * 1. Run with `spark.sql.catalog.oracle.use_resultset_cache=false` in [[TestOracleHive]]
 *    Setting it here doesn't get applied. [[ResultSetCache]] gets initialized once.
 * 2. Run with `spark.sql.catalog.oracle.use_metadata_cache_only=false` so that the
 *    `sparktest.sparktest_sales` is read.
 *
 * Todo:
 * Tests:
 * - unit_test_p scan  (DONE)
 * - ss scan with filter
 * - sparktest.sparktest_sales scan (DONE)
 * - star-join
 * - star-join-agg resulting in result split
 * - outer-joins split ?
 * - why is OraScan.planInputPartitions called twice
 *
 * - run on all tpcds

 */
abstract class QuerySplittingTest extends AbstractTest
  with PlanTestHelpers with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    super.beforeAll()
    setupSplitting(true, split_100k)
    TestOracleHive.sql("set spark.sql.catalog.oracle.use_resultset_cache=false")
  }

  override def afterAll(): Unit = {
    TestOracleHive.sql("set spark.sql.catalog.oracle.use_resultset_cache=true")
    setupSplitting(false, split_1m)
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    setupSplitting(true, split_100k)
  }

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

  ignore("partitionSplit") { td =>

  val sql =
    """
      |select *
      |from sparktest.unit_test_partitioned""".stripMargin

    val df1 = collect(sql)(true, split_100k)
    val df2 = collect(sql)(false, split_100k)

    isTwoDataFrameEqual(df1, df2, 0.0)

  }

  test("rowIdSplit") { td =>

    val sql =
      """
        |select *
        |from sparktest.sparktest_sales
        |where amount_sold > 1000""".stripMargin

    val df1 = collect(sql)(true, split_100k)
    val df2 = collect(sql)(false, split_100k)

    isTwoDataFrameEqual(df1, df2, 0.0)

  }

}
