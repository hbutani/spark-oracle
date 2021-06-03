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

package org.apache.spark.sql.oracle.rules.sharding

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.oracle.sharding.ShardQueryInfo
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * transfer the [[ShardQueryInfo]] tag from a [[DataSourceV2ScanRelation]]
 * to the associated [[OraPlan]]
 */
object AnnotateOraPlan extends OraShardingLogicalRule
  with AnnotatePredicateHelper
  with Logging {

  override def _apply(plan: LogicalPlan)(implicit sparkSession: SparkSession): LogicalPlan = {

    logDebug(s"applying AnnotateOraPlan on ${plan.treeString}")

    plan foreachUp {
      case ds@DataSourceV2ScanRelation(_, oraScan: OraScan, _) =>
        for (sInfo <- ShardQueryInfo.getShardingQueryInfo(ds)) {
          ShardQueryInfo.setShardingQueryInfo(oraScan.oraPlan, sInfo)
        }
      case _ => ()
    }
    plan
  }
}
