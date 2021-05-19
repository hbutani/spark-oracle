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
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project}
import org.apache.spark.sql.connector.catalog.oracle.OracleCatalog
import org.apache.spark.sql.connector.catalog.oracle.sharding._
import org.apache.spark.sql.connector.catalog.oracle.sharding.ShardQueryInfo._
import org.apache.spark.sql.connector.read.oracle.{OraFileScan, OraPushdownScan, OraScan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

object AnnotateShardingInfoRule
    extends OraShardingLogicalRule
    with PredicateHelper
    with Logging
    with FilterAnnotate
    with JoinAnnotate
    with AggregateAnnotate {

  private[sql] def project(from: LogicalPlan, proj: Project)(
      implicit sparkSession: SparkSession,
      shardedMD: ShardingMetadata): Unit = {
    val sInfo = getShardingQueryInfoOrCoord(from)
    proj.setTagValue(ORA_SHARDING_QUERY_TAG, sInfo)
  }

  private def annotate(plan: LogicalPlan)(
      implicit sparkSession: SparkSession,
      shardedMD: ShardingMetadata): LogicalPlan =
    plan transformUp {
      case plan if ShardQueryInfo.hasShardingQueryInfo(plan) => plan
      case ds @ DataSourceV2ScanRelation(_, oraFScan: OraFileScan, _) =>
        val oraTab = oraFScan.oraPlan.oraTable
        val shardQInfo = shardedMD.shardQueryInfo(oraTab)
        logInfo(s"annotate DSV2 for ${oraTab.name} with ShardQueryInfo ${shardQInfo.show}")
        ShardQueryInfo.setShardingQueryInfo(ds, shardQInfo)
        ds
      case ds @ DataSourceV2ScanRelation(_, oraScan: OraPushdownScan, _) =>
        val oraPlan = oraScan.oraPlan
        val catalystOp = oraPlan.catalystOp
        if (catalystOp.isDefined) {
          val catOp = catalystOp.get
          annotate(catOp)
          ShardQueryInfo.getShardingQueryInfo(catOp).map { sQI =>
            ShardQueryInfo.setShardingQueryInfo(ds, sQI)
          }
        } else {
          ShardQueryInfo.setShardingQueryInfo(ds, shardedMD.COORD_QUERY_INFO)
        }
        ds
      case p @ Project(_, child) =>
        project(child, p)
        p
      case f @ Filter(_, child) =>
        filter(child, f)
        f
      case joinOp @ ExtractEquiJoinKeys(
            joinType,
            leftKeys,
            rightKeys,
            condition,
            leftChild,
            rightChild,
            _) =>
        equiJoin(leftChild, rightChild, leftKeys, rightKeys, condition, joinOp)
        joinOp
      case aggOp @ Aggregate(_, _, child) =>
        aggregate(child, aggOp)
        aggOp
      case op =>
        ShardQueryInfo.setShardingQueryInfo(op, shardedMD.COORD_QUERY_INFO)
        op
    }

  override def _apply(plan: LogicalPlan)(implicit sparkSession: SparkSession): LogicalPlan = {
    val oraCatalog =
      sparkSession.sessionState.catalogManager.catalog("oracle").asInstanceOf[OracleCatalog]
    implicit val shardedMD = oraCatalog.getMetadataManager.getShardingMetadata

    logDebug(s"applying AnnotateShardingInfoRule on ${plan.treeString}")

    plan transformUp {
      case ds @ DataSourceV2ScanRelation(_, oraScan: OraScan, _) =>
        annotate(ds)
        ds
    }
  }
}
