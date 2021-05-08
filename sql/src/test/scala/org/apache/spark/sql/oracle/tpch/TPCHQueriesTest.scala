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

package org.apache.spark.sql.oracle.tpch

import org.apache.spark.sql.connector.catalog.oracle.ShardingAbstractTest
import org.apache.spark.sql.hive.test.oracle.TestOracleHive

class TPCHQueriesTest extends ShardingAbstractTest {

  // scalastyle:off println
  test("showPlans") { td =>

    TestOracleHive.sql(s"set spark.sql.oracle.enable.querysplitting=true")
    TestOracleHive.sql(s"set spark.sql.oracle.querysplit.target=1mb")
    TestOracleHive.sql(s"set spark.sql.oracle.allow.splitresultset=true")

    /*
   * Not fully pushed queries:
   * q21 -> exists and not exists
   */

    for ((qNm, q) <- TPCHQueries.queries) {
      println(s"Query ${qNm}:")
      TestOracleHive.sql(s"explain oracle pushdown $q").show(10000, false)
    }
  }

  // scalastyle:on

}
