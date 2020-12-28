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

package org.apache.spark.sql.oracle.translation

import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.tpcds.TPCDSQueries


class WindowTranslationTests extends AbstractTranslationTest {

  val windowTestQueries = Seq(
    "q12", "q20", "q44", "q47", "q49", "q51", "q53", "q57",
    "q63", "q67", "q89", "q98")

  // TODO: handle bit shift expression from spark grouping function
  val notPushdown = Seq("q36", "q70", "q86")

  for ((qNm, q) <- TPCDSQueries.queries if windowTestQueries.contains(qNm)) {
    test(qNm) { td =>
      val plan = TestOracleHive.sql(q.sql).queryExecution.optimizedPlan
      assert(plan.isInstanceOf[DataSourceV2ScanRelation])
    }
  }
}
