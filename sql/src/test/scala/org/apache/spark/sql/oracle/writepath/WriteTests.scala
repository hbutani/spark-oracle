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

import org.apache.spark.sql.hive.test.oracle.TestOracleHive


abstract class WriteTests extends AbstractWriteTests {

  import AbstractWriteTests._

  ignore("printScenarioDetails") {td =>
    val scnInfos = scenarios.map(scenarioInfo)
    val (scnSummaries, scnPlans) = scnInfos.unzip

    // scalastyle:off println
    println(
      s"""Scenarios:
         |${scnSummaries.mkString("\n")}""".stripMargin)
    println(
      s"""Scenario Plans:
         |${scnPlans.mkString("\n")}""".stripMargin)

    // scalastyle:on println
  }

  performTest(scenarios(0))
  performTest(scenarios(1))
  // performTest(scenarios(2))
  performTest(scenarios(3))
  performTest(scenarios(4))
  performTest(scenarios(5))
  performTest(scenarios(6))
  performTest(scenarios(7))
  performTest(scenarios(8))
  // performTest(scenarios(9))

  // run to validate src_tab_for_writes data file
  ignore("validateSrcData") {t =>
    try {
      TestOracleHive.sql("use spark_catalog")
      TestOracleHive.sql("select * from default.src_tab_for_writes").show(1000, false)
    } finally {
      TestOracleHive.sql("use oracle.sparktest")
    }
  }


}
