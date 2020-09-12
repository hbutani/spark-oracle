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

package org.apache.spark.sql.connector.catalog.oracle

import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.AbstractTest

/*
run with params:
-Dspark.oracle.test.db_instance=mammoth_medium
-Dspark.oracle.test.db_wallet_loc=/Users/hbutani/oracle/wallet_mammoth
 */
class LoadMetadataCache extends AbstractTest {

  lazy val mdMgr = {
    val catMgr = TestOracleHive.sparkSession.sessionState.analyzer.catalogManager
    val oraCat = catMgr.catalog("oracle").asInstanceOf[OracleCatalog]
    oraCat.getMetadataManager
  }

  test("populateMetadataCache") { td =>
    for ((_, tbls) <- mdMgr.tableMap;
         tbl <- tbls) {
      // scalastyle:off println
      println(s"${tbl.namespace().head} ${tbl.name()}")
      println(mdMgr.oraTableFromDB(tbl))
    }
  }

}
