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

package org.apache.spark.sql.oracle

import org.scalatest.{Suite, Suites}

import org.apache.spark.sql.connector.catalog.oracle.sharding.{IntervalTreeTest, ShardingMetadataTest}
import org.apache.spark.sql.oracle.tpch.TPCHQueriesTest
import org.apache.spark.sql.oracle.translation.sharding.{ShardingAnnotationTest, ShardingJoinAnnotationTest}

class ShardingTestSuite extends Suites(ShardingTestSuite.tests : _*)

object ShardingTestSuite {
  val tests : Seq[Suite] = Seq(
    new ShardingMetadataTest(),
    new IntervalTreeTest(),
    new TPCHQueriesTest(),
    new ShardingAnnotationTest(),
    new ShardingJoinAnnotationTest()
  )
}
