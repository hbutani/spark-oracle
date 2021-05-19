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

package org.apache.spark.sql.oracle.translation.sharding

import org.apache.spark.sql.connector.catalog.oracle.sharding.{ShardedQuery, ShardQueryInfo}
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.tpch.TPCHQueries

class ShardingAnnotationTest extends AbstractShardingTranslationTest {

  // scalastyle:off println

  def showAnnotation(q: String): Unit = {
    val plan = TestOracleHive.sql(q).queryExecution.optimizedPlan
    println(showShardingAnnotation(plan))
  }

  def checkShardingInfo(q: String, shardSet: Set[Int]): Unit = {
    val plan = TestOracleHive.sql(q).queryExecution.optimizedPlan
    val sInfo = ShardQueryInfo.getShardingQueryInfo(plan)
    assert(
      sInfo.isDefined &&
        sInfo.get.queryType == ShardedQuery &&
        sInfo.get.shardInstances == shardSet)
  }

  ignore("showShardingQInfo") { td =>
    TestOracleHive.sql(s"set spark.sql.oracle.enable.querysplitting=true")
    TestOracleHive.sql(s"set spark.sql.oracle.querysplit.target=1mb")
    TestOracleHive.sql(s"set spark.sql.oracle.allow.splitresultset=true")

    for ((qNm, q) <- TPCHQueries.queries) {
      println(s"Query ${qNm}:")
      val plan = TestOracleHive.sql(s"$q").queryExecution.optimizedPlan
      println(showShardingAnnotation(plan))
    }
  }

  test("basicQ") { td =>
    showAnnotation("select l_returnflag from lineitem")

    checkShardingInfo("select l_returnflag from lineitem where l_orderkey = 10", Set(0))

    checkShardingInfo("select l_returnflag from lineitem where l_orderkey <= 100", Set(0))

    checkShardingInfo(
      "select l_returnflag from lineitem where l_orderkey = 2 * 100 * 1000",
      Set(1))

    checkShardingInfo(
      "select l_returnflag from lineitem where l_orderkey >= 2 * 100 * 1000",
      Set(1, 2))

    checkShardingInfo(
      "select l_returnflag from lineitem where l_orderkey >= 10 * 1000 * 1000",
      Set(2))

    checkShardingInfo(
      "select l_returnflag from lineitem where l_orderkey = 10 * 1000 * 1000",
      Set(2))

    checkShardingInfo(
      "select l_returnflag from lineitem where l_orderkey > 100 and l_orderkey <= 2 * 100 * 1000",
      Set(0, 1))

    checkShardingInfo(
      "select l_returnflag from lineitem where l_orderkey in (100, 2 * 100 * 1000)",
      Set(0, 1))

    checkShardingInfo(
      "select l_returnflag from lineitem where l_orderkey in (10 * 1000 * 1000, 2 * 100 * 1000)",
      Set(1, 2))

    checkShardingInfo("select l_returnflag from lineitem where 100 >= l_orderkey", Set(0))

    checkShardingInfo(
      "select l_returnflag from lineitem where 100 < l_orderkey and  2 * 100 * 1000 >= l_orderkey",
      Set(0, 1))
  }

  // scalastyle:on

}
