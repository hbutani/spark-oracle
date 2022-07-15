/*
  Copyright (c) 2021, Oracle and/or its affiliates.

  This software is dual-licensed to you under the Universal Permissive License
  (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
  2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
  either license.

  If you elect to accept the software under the Apache License, Version 2.0,
  the following applies:

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/
package org.apache.spark.sql.oracle.translation

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.{AbstractTest, OraSparkConfig, PlanTestHelpers}
import org.apache.spark.sql.oracle.operators.OraQueryBlock

abstract class AbstractTranslationTest extends AbstractTest with PlanTestHelpers {
  import AbstractTranslationTest._

  def testPushdown(nm : String,
                   sqlStr : String,
                   oraSQL : String = null,
                   showPlan: Boolean = false,
                   showResults: Boolean = false) : Unit = {
    test(nm) { td =>
      val df = TestOracleHive.sql(sqlStr)
      val scans = collectScans(df.queryExecution.optimizedPlan)
      assert(scans.size == 1)
      if (oraSQL == null) {
        scans(0).showOraSQL(nm)
      } else {
        assert(scans(0).oraPlan.orasql.sql == oraSQL)
      }
      doSQL(nm, Right(df), showPlan, showResults, 1000)
    }
  }

  /**
   * Use utility to debug pushdown issues.
   *  - shows non-pushdown plan
   *  - pushdown plan should only have 1 OraScan; shows pushdown sql
   *  - checks that non-pushdown and pushdown results are the same
   *
   * @param testName
   * @param sqlStr
   */
  def debugPushdown(testName : String,
                    sqlStr : String) : Unit = {
    OraSparkConfig.setConf(OraSparkConfig.ENABLE_ORA_PUSHDOWN, false)
    val df1 = TestOracleHive.sql(sqlStr)

    // scalastyle:off println
    System.out.println(
      s"""Non pushdown plan:
         |${df1.queryExecution.optimizedPlan}""".stripMargin)
    // scalastyle:on println

    OraSparkConfig.setConf(OraSparkConfig.ENABLE_ORA_PUSHDOWN, true)
    val df2 = TestOracleHive.sql(sqlStr)
    val scans = collectScans(df2.queryExecution.optimizedPlan)
    assert(scans.size == 1)
    scans(0).showOraSQL(testName)

    isTwoDataFrameEqual(df1, df2, 0.0, false, true, System.out)

    if (false) {
      doSQL(testName, Right(df1), true, true, 1000)
      doSQL(testName, Right(df2), true, true, 1000)
    }
  }
}

object AbstractTranslationTest {
  case class Pushdown(dsScan: DataSourceV2ScanRelation,
                      oraScan: OraScan,
                      oraPlan: OraQueryBlock
                     ) {
    // scalastyle:off println
    def showOraSQL(tNm : String) : Unit = {
      println(
        s"""Oracle SQL for test '${tNm}' :
           |${oraPlan.orasql.sql}""".stripMargin
      )
    }
    // scalastyle:on println
  }

  def collectScans(plan : LogicalPlan) : Seq[Pushdown] = plan collect {
    case dsv2@DataSourceV2ScanRelation(_, oraScan: OraScan, _) =>
      Pushdown(dsv2, oraScan, oraScan.oraPlan.asInstanceOf[OraQueryBlock])
  }
}
