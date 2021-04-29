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

import oracle.spark.ConnectionManagement
import org.scalactic.source
import org.scalatest.Tag

import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.AbstractTest

abstract class ShardingAbstractTest extends AbstractTest {

  lazy val dsKey = OracleCatalog.oracleCatalog.getMetadataManager.dsKey
  lazy val isTestInstanceSharded = ConnectionManagement.info(dsKey).isSharded

  class ShardResultOfTestInvocation(testName: String, testTags: Tag*)
    extends ResultOfTestInvocation(testName, testTags : _*) {
    override def apply(testFun: FixtureParam => Any /* Assertion */)
                      (implicit pos: source.Position): Unit = {
      super.apply { fixParam =>
        if (isTestInstanceSharded) {
          testFun(fixParam)
        }
      }
    }

    override def apply(testFun: () => Any /* Assertion */)
                      (implicit pos: source.Position): Unit = {
      super.apply { () =>
        if (isTestInstanceSharded) {
          testFun()
        }
      }
    }
  }

  override protected def test(testName: String, testTags: Tag*): ResultOfTestInvocation = {
    new ShardResultOfTestInvocation(testName, testTags: _*)
  }

  override def afterAll(): Unit = {
    if (isTestInstanceSharded) {
      TestOracleHive.sql("use tpch")
    } else {
      super.afterAll()
    }
  }
}
