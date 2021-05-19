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

package org.apache.spark.sql.connector.catalog.oracle.sharding

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.oracle.querysplit.PlanInfo

sealed trait ShardQueryType

case object ReplicatedQuery extends ShardQueryType
case object CoordinatorQuery extends ShardQueryType
case object ShardedQuery extends ShardQueryType

case class ShardQueryInfo(queryType : ShardQueryType,
                          shardTable : Option[ShardTable],
                          shardInstances : Set[Int],
                          planInfo : Option[PlanInfo]
                         ) {

  def show(append: String => Unit) : Unit = {
    append(queryType.toString)
    if (queryType == ShardedQuery) {
      append(s", shardTable = ${shardTable.get.qName.toString()}")
      append(s", tableFamily = ${shardTable.get.tableFamilyId}")
      append(s", shardInstances = ${shardInstances.mkString("[", ", ", "]")}")
    }
  }

  def show : String = {
    val sb = new StringBuilder
    show( s => sb.append(s))
    sb.toString()
  }
}

object ShardQueryInfo {
  val ORA_SHARDING_QUERY_TAG = TreeNodeTag[ShardQueryInfo]("_ShardingQueryInfo")

  private[sql] def hasShardingQueryInfo(plan : LogicalPlan) : Boolean =
    getShardingQueryInfo(plan).isDefined

  private[sql] def getShardingQueryInfo(plan : LogicalPlan) : Option[ShardQueryInfo] =
    plan.getTagValue(ORA_SHARDING_QUERY_TAG)

  private[sql] def getShardingQueryInfoOrCoord(plan : LogicalPlan)
                                              (implicit sparkSession: SparkSession,
                                               shardedMD : ShardingMetadata
                                              ): ShardQueryInfo =
    plan.getTagValue(ORA_SHARDING_QUERY_TAG).getOrElse(shardedMD.COORD_QUERY_INFO)

  private[sql] def setShardingQueryInfo(plan : LogicalPlan, sInfo : ShardQueryInfo) : Unit =
    plan.setTagValue(ORA_SHARDING_QUERY_TAG, sInfo)
}
