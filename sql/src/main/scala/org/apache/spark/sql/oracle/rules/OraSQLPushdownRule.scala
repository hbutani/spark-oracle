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
import org.apache.spark.sql.connector.read.oracle.{OraPushdownScan, OraScan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.expressions.{AND, OraBinaryOpExpression, OraExpression, OraExpressions}
import org.apache.spark.sql.oracle.operators.{OraPlan, OraQueryBlock, OraTableScan}

object OraSQLPushdownRule extends OraLogicalRule with Logging {

  /**
   * Setting up a [[OraQueryBlock]] for a [[OraTableScan]]:
   * - the [[OraExpression]]s in the [[OraTableScan]] refer to
   * [[org.apache.spark.sql.catalyst.expressions.AttributeReference]]s that have
   * different [[org.apache.spark.sql.catalyst.expressions.ExprId]]s from
   * the once in the [[DataSourceV2ScanRelation#output]].
   * - this should be ok; as long as we capture the [[DataSourceV2ScanRelation#output]]
   *   as the `catalystOutput` of the constructed [[OraQueryBlock]]
   *
   * @param oraPlan
   * @param dsv2
   * @return
   */
  private def toOraQueryBlock(oraPlan : OraPlan,
                      dsv2: DataSourceV2ScanRelation) : OraQueryBlock = oraPlan match {
    case oraTScan : OraTableScan =>
      val oraProjs = OraExpressions.unapplySeq(dsv2.output).get
      val fils = (oraTScan.filter.toSeq ++ oraTScan.partitionFilter.toSeq)
      val oraFil = if (fils.nonEmpty) {
        Some(
          fils.reduceLeft[OraExpression] {
          case (l, r) => OraBinaryOpExpression(AND, l.catalystExpr, l, r)
        }
        )
      } else None
      OraQueryBlock(oraTScan, Seq.empty, oraProjs, oraFil, None, Some(dsv2), dsv2.output)
    case oraQBlck : OraQueryBlock => oraQBlck
  }

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

    def withOraQBlock(dsv2: DataSourceV2ScanRelation) : DataSourceV2ScanRelation = {
      val oraScan : OraScan = dsv2.scan.asInstanceOf[OraScan]
      val oraQBlk : OraQueryBlock = toOraQueryBlock(oraScan.oraPlan, dsv2)
      dsv2.copy(scan = OraPushdownScan(oraScan.sparkSession, oraScan.dsKey, oraQBlk))
    }

    val pushPlans = getLastProjectFilter(scanPlan, dsV2, projections.nonEmpty, filters.nonEmpty)
    /*
     * set the [[OraPlan]] in the [[DataSourceV2ScanRelation]] as a [[OraQueryBlock]]
     */
    val dsV2WithOraQBlock = withOraQBlock(dsV2)

    val pushedDSV2 = pushPlans.foldLeft(Some(dsV2WithOraQBlock): Option[DataSourceV2ScanRelation]) {
      case (None, _) => None
      case (Some(dsV2@DataSourceV2ScanRelation(_, oraScan: OraScan, _)), p: Project) =>
        ProjectPushdown(dsV2,
          oraScan: OraScan,
          oraScan.oraPlan.asInstanceOf[OraQueryBlock],
          p,
          sparkSession: SparkSession
        ).pushdown
      case (Some(dsV2@DataSourceV2ScanRelation(_, oraScan: OraScan, _)), f: Filter) =>
        FilterPushdown(dsV2,
          oraScan: OraScan,
          oraScan.oraPlan.asInstanceOf[OraQueryBlock],
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
      rightChild@DataSourceV2ScanRelation(_, oraScanR : OraScan, _),
      _
      ) => joinType match {
        case LeftAnti | LeftSemi =>
          SemiAntiJoinPushDown(leftChild, oraScanL, toOraQueryBlock(oraScanL.oraPlan, leftChild),
            toOraQueryBlock(oraScanR.oraPlan, rightChild), joinOp, joinType, leftKeys, rightKeys,
            condition, sparkSession
            ).pushdown.getOrElse(joinOp)
        case _ =>
          JoinPushDown(leftChild, oraScanL, toOraQueryBlock(oraScanL.oraPlan, leftChild),
            toOraQueryBlock(oraScanL.oraPlan, leftChild), joinOp, joinType, leftKeys, rightKeys,
            condition, sparkSession
          ).pushdown.getOrElse(joinOp)
      }
      case aggOp@Aggregate(_, _, child@DataSourceV2ScanRelation(_, oraScan : OraScan, _)) =>
        AggregatePushDown(child, oraScan,
          toOraQueryBlock(oraScan.oraPlan, child), aggOp, sparkSession).
          pushdown.getOrElse(aggOp)
      case expOp@Expand(_, _, child@DataSourceV2ScanRelation(_, oraScan : OraScan, _)) =>
        ExpandPushDown(child, oraScan,
          toOraQueryBlock(oraScan.oraPlan, child), expOp, sparkSession).
          pushdown.getOrElse(expOp)
    }
  }
}
