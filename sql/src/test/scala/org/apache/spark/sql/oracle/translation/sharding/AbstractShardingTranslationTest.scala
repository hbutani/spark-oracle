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

package org.apache.spark.sql.oracle.translation.sharding

import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.connector.catalog.oracle.sharding.{ReplicatedQuery, ShardedQuery, ShardQueryInfo}
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.ShardingAbstractTest

class AbstractShardingTranslationTest extends ShardingAbstractTest {

  private def generateTreeString(
      plan: LogicalPlan,
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit): Unit = {

    def genForChildren(children: Seq[QueryPlan[_]]): Unit = {
      if (children.nonEmpty) {
        children.init.foreach(
          c =>
            generateTreeString(
              c.asInstanceOf[LogicalPlan],
              depth + 2,
              lastChildren :+ children.isEmpty :+ false,
              append))

        generateTreeString(
          children.last.asInstanceOf[LogicalPlan],
          depth + 2,
          lastChildren :+ plan.children.isEmpty :+ true,
          append)
      }
    }

    append("   ")
    if (depth > 0) {
      lastChildren.init.foreach { isLast =>
        append(if (isLast) "   " else ":  ")
      }
      append(if (lastChildren.last) "+- " else ":- ")
    }

    append(plan.simpleStringWithNodeId())
    val sQInfo = ShardQueryInfo.getShardingQueryInfo(plan)
    if (sQInfo.isDefined) {
      append("  ")
      sQInfo.get.show(append)
    }
    append("\n")

    genForChildren(plan.innerChildren)
    genForChildren(plan.children)
  }

  def showShardingAnnotation(plan: LogicalPlan): String = {
    val concat = new PlanStringConcat()
    generateTreeString(plan, 0, Nil, concat.append)
    concat.toString()
  }

  // scalastyle:off println

  def showAnnotation(q: String): Unit = {
    val plan = TestOracleHive.sql(q).queryExecution.optimizedPlan
    println(showShardingAnnotation(plan))
  }

  def checkShardingInfo(q: String, shardSet: Set[Int]): Unit = {
    val plan = TestOracleHive.sql(q).queryExecution.optimizedPlan
    val sInfo = ShardQueryInfo.getShardingQueryInfo(plan)
    assert(
      sInfo.isDefined &&
        sInfo.get.queryType == ShardedQuery &&
        sInfo.get.shardInstances == shardSet)
  }

  def checkReplicatedQuery(q: String): Unit = {
    val plan = TestOracleHive.sql(q).queryExecution.optimizedPlan
    val sInfo = ShardQueryInfo.getShardingQueryInfo(plan)
    assert(
      sInfo.isDefined &&
        sInfo.get.queryType == ReplicatedQuery)
  }

  // scalastyle:on println

}
