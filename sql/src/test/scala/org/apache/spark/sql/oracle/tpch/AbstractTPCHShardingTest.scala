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

import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.translation.sharding.AbstractShardingTranslationTest

abstract class AbstractTPCHShardingTest extends AbstractShardingTranslationTest {

  val split_1k = "1kb"
  val split_10k = "10kb"
  val split_100k = "100kb"
  val split_1m = "1Mb"

  def setupSplitting(implicit qSplit : Boolean, splitTarget : String) : Unit = {
    TestOracleHive.sql(s"set spark.sql.oracle.enable.querysplitting=${qSplit}")
    TestOracleHive.sql(s"set spark.sql.oracle.querysplit.target=${splitTarget}")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    setupSplitting(true, split_100k)
    TestOracleHive.sql("set spark.sql.oracle.allow.splitresultset=true")
  }

  override def afterAll(): Unit = {
    TestOracleHive.sql("set spark.sql.oracle.allow.splitresultset=false")
    setupSplitting(false, split_100k)
    super.afterAll()
  }

}
