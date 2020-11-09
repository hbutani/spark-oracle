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

package org.apache.spark.sql.oracle.tpcds

import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.{AbstractTest, PlanTestHelpers}
import org.apache.spark.sql.oracle.tpcds.TPCDSQueries.TPCDSQuerySpec

class CheckScansTest extends AbstractTest with PlanTestHelpers {

  def performTest(qNm: String, q: TPCDSQuerySpec): Unit = {
    test(qNm) { _ =>
      /*
      TODO: recreate ScanDetails

      validateOraScans(q.sql, q.scanDetailsMap)
      TestOracleHive.sql(s"explain formatted ${q.sql}").show(false)

       */

      showOraQueries(qNm, q.sql)

    }
  }

  for ((qNm, q) <- TPCDSQueries.queries) {
    performTest(qNm, q)
  }

}
