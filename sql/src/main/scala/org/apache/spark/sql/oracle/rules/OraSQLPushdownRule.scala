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
package org.apache.spark.sql.oracle.rules

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

class OraSQLPushdownRule extends OraLogicalRule with Logging {

  private def pushProjectFilters(dsV2: DataSourceV2ScanRelation,
                                 scanPlan: LogicalPlan,
                                 projections: Seq[NamedExpression],
                                 filters: Seq[Expression]
                                )(implicit sparkSession: SparkSession): LogicalPlan = {

    def getLastProjectFilter(endplan: LogicalPlan,
                             startPlan: LogicalPlan,
                             hasProjects: Boolean,
                             hasFilters: Boolean): Seq[LogicalPlan] = {
      var p = endplan
      var prjFnd = !hasProjects
      var filFnd = !hasFilters
      var s = Seq.empty[LogicalPlan]
      while ((!prjFnd || !filFnd) && (p != startPlan)) {
        p match {
          case pr: Project if !prjFnd =>
            s = pr +: s
            prjFnd = true
          case fl: Filter if !filFnd =>
            s = fl +: s
            filFnd = true
          case _ => ()
        }
        p = p.children.head
      }
      s
    }

    val pushPlans = getLastProjectFilter(scanPlan, dsV2, projections.nonEmpty, filters.nonEmpty)

    val pushedDSV2 = pushPlans.foldLeft(Some(dsV2): Option[DataSourceV2ScanRelation]) {
      case (None, _) => None
      case (Some(dsV2@DataSourceV2ScanRelation(_, oraScan: OraScan, _)), p: Project) =>
        ProjectPushdown(dsV2,
          oraScan: OraScan,
          oraScan.oraPlan.toOraQueryBlock,
          p,
          sparkSession: SparkSession
        ).pushdown
      case (Some(dsV2@DataSourceV2ScanRelation(_, oraScan: OraScan, _)), f: Filter) =>
        FilterPushdown(dsV2,
          oraScan: OraScan,
          oraScan.oraPlan.toOraQueryBlock,
          f,
          sparkSession: SparkSession
        ).pushdown
      case _ => None
    }

    pushedDSV2.getOrElse(scanPlan)
  }

  override def _apply(plan: LogicalPlan)(implicit sparkSession: SparkSession): LogicalPlan = {

    /* First collapse Project-Filter by transformDown */

    val plan1 = plan transformDown {
      case scanPlan@PhysicalOperation(projections, filters, dsV2: DataSourceV2ScanRelation) =>
        pushProjectFilters(dsV2, scanPlan, projections, filters)
    }

    plan1 transformUp {
      case scanPlan@PhysicalOperation(projections, filters, dsV2: DataSourceV2ScanRelation) =>
        pushProjectFilters(dsV2, scanPlan, projections, filters)
      case joinOp@ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition,
      leftChild@DataSourceV2ScanRelation(_, oraScanL : OraScan, _),
      DataSourceV2ScanRelation(_, oraScanR : OraScan, _),
      _
      ) => joinType match {
        case LeftAnti | LeftSemi =>
          SemiAntiJoinPushDown(leftChild, oraScanL, oraScanL.oraPlan.toOraQueryBlock,
            oraScanR.oraPlan.toOraQueryBlock, joinOp, joinType, leftKeys, rightKeys,
            condition, sparkSession
            ).pushdown.getOrElse(joinOp)
        case _ =>
          JoinPushDown(leftChild, oraScanL, oraScanL.oraPlan.toOraQueryBlock,
            oraScanR.oraPlan.toOraQueryBlock, joinOp, joinType, leftKeys, rightKeys,
            condition, sparkSession
          ).pushdown.getOrElse(joinOp)
      }
      case aggOp@Aggregate(_, _, child@DataSourceV2ScanRelation(_, oraScan : OraScan, _)) =>
        AggregatePushDown(child, oraScan, oraScan.oraPlan.toOraQueryBlock, aggOp, sparkSession).
          pushdown.getOrElse(aggOp)
      case expOp@Expand(_, _, child@DataSourceV2ScanRelation(_, oraScan : OraScan, _)) =>
        ExpandPushDown(child, oraScan, oraScan.oraPlan.toOraQueryBlock, expOp, sparkSession).
          pushdown.getOrElse(expOp)
    }
  }
}
