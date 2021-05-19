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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.connector.catalog.oracle.sharding._
import org.apache.spark.sql.connector.catalog.oracle.sharding.ShardQueryInfo._

trait JoinAnnotate { self: AnnotateShardingInfoRule.type =>

  private[sql] def equiJoin(
      left: LogicalPlan,
      right: LogicalPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinCond: Option[Expression],
      joinOp: Join)(implicit sparkSession: SparkSession, shardedMD: ShardingMetadata): Unit = {
    val sInfoLeft = getShardingQueryInfoOrCoord(left)
    val sInfoRight = getShardingQueryInfoOrCoord(right)

    /*
     * If both sides are on the same tableFamily
     * - check if join is on shardingKeys
     */

    ShardQueryInfo.setShardingQueryInfo(joinOp, shardedMD.COORD_QUERY_INFO)

  }

}
