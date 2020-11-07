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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.{AbstractTest, PlanTestHelpers}
import org.apache.spark.sql.oracle.operators.OraQueryBlock

abstract class AbstractTranslationTest extends AbstractTest with PlanTestHelpers {
  import AbstractTranslationTest._

  def testPushdown(nm : String,
                   sqlStr : String,
                  oraSQL : String = null) : Unit = {
    test(nm) { td =>
      val df = TestOracleHive.sql(sqlStr)
      val scans = collectScans(df.queryExecution.optimizedPlan)
      assert(scans.size == 1)
      if (oraSQL == null) {
        scans(0).showOraSQL(nm)
      } else {
        assert(scans(0).oraPlan.orasql.sql == oraSQL)
      }
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
