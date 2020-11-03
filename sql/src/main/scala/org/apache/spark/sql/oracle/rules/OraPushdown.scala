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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AliasHelper, And, AttributeReference, Expression, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.read.oracle.{OraPushdownScan, OraScan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.expressions.{AND, OraBinaryOpExpression, OraExpression, OraExpressions}
import org.apache.spark.sql.oracle.operators.OraQueryBlock

// scalastyle:off
/**
 * '''Table of Pushdown Constraints:'''
 * - Constraints on applying a relational operation in an existing [[OraQueryBlock]]
 * - yes means that the operation can be part of the same [[OraQueryBlock]]
 * - no means that a new [[OraQueryBlock]] is setup on which the operation is
 * applied. Existing Query Block becomes the source of this [[OraQueryBlock]]
 * {{{
 *QBlk has/  Column  Proj  Filt  Join  O-Join  S-Join  A-Join  L-Join  Agg
 *Apply      Pruned
 *--------------------------------------------------------------------------
 *Col. Prun  yes     yes   yes   yes   yes     yes     yes     yes     yes
 *Proj       yes     yes   yes   yes   yes     yes     yes     yes     yes
 *Filt       yes     yes   yes   yes   no      yes     no      yes     no
 *Join       yes     no    yes   yes   yes     no      no      no      no
 *Out-Join   yes     no    yes   yes   yes     yes     yes     yes     no
 *Semi-Join  yes     no    yes   yes   no      yes     yes     yes     no
 *Anti-Join
 *Lat-Join
 *Agg
 * }}}
 *
 * - Application of Project requires that AttrRefs in projections be substituted based
 * on current projections
 * - Application of Filter requires that AttrRefs in projections be substituted based
 * on current projections
 */
trait OraPushdown {
  // scalastyle:on
  val inDSScan: DataSourceV2ScanRelation
  val inOraScan: OraScan
  val inQBlk: OraQueryBlock
  val pushdownCatalystOp: LogicalPlan
  val sparkSession: SparkSession

  lazy val currQBlk = if (inQBlk.canApply(pushdownCatalystOp)) {
    inQBlk
  } else {
    inQBlk.newBlockOnCurrent
  }

  private[rules] def pushdownSQL: Option[OraQueryBlock]

  def pushdown: Option[DataSourceV2ScanRelation] = {
    pushdownSQL.map { oraPlan =>
      val newOraScan = OraPushdownScan(sparkSession, inOraScan.dsKey, oraPlan)
      inDSScan.copy(
        scan = newOraScan,
        output = pushdownCatalystOp.output.asInstanceOf[Seq[AttributeReference]]
      )
    }
  }

}

case class ProjectPushdown(inDSScan: DataSourceV2ScanRelation,
                           inOraScan: OraScan,
                           inQBlk: OraQueryBlock,
                           pushdownCatalystOp: Project,
                           sparkSession: SparkSession)
  extends OraPushdown with AliasHelper {

  /**
   * copied from [[CollapseProject]] rewrite rule.
   * - substitues refernces to aliases and `trimNonTopLevelAliases`
   * For example:
   * {{{
   *   Project(c + d as e,
   *           Project(a + b as c, d, DSV2...)
   *           )
   *  // becomes
   *  Project(a + b +d as e, DSV2...)
   * }}}
   * @param upper
   * @param lower
   * @return
   */
  def buildCleanedProjectList(upper: Seq[NamedExpression],
                              lower: Seq[NamedExpression]): Seq[NamedExpression] = {
    val aliases = getAliasMap(lower)
    upper.map(replaceAliasButKeepName(_, aliases))
  }

  private[rules] def pushdownSQL: Option[OraQueryBlock] = {
    if (inQBlk.canApply(pushdownCatalystOp)) {
      val projOp = pushdownCatalystOp
      val pushdownProjList =
        buildCleanedProjectList(projOp.projectList, currQBlk.catalystProjectList)
      for(oraExpressions <- OraExpressions.unapplySeq(pushdownProjList)) yield {
        currQBlk.copy(select = oraExpressions,
          catalystOp = Some(projOp),
          catalystProjectList = projOp.projectList)
      }
    } else None
  }

}

case class FilterPushdown(inDSScan: DataSourceV2ScanRelation,
                          inOraScan: OraScan,
                          inQBlk: OraQueryBlock,
                          pushdownCatalystOp: Filter,
                          sparkSession: SparkSession)
  extends OraPushdown with PredicateHelper {

  private[rules] def pushdownSQL: Option[OraQueryBlock] = {
    if (inQBlk.canApply(pushdownCatalystOp)) {
      val filOp = pushdownCatalystOp

      val pushdownCondition =
        replaceAlias(filOp.condition, getAliasMap(currQBlk.catalystProjectList))
      val pushdownFilters = {
        val splitConds = splitConjunctivePredicates(pushdownCondition)
        val currConds: Set[Expression] = currQBlk.where.map { oraCond =>
          oraCond.collect {
            case oE => oE.catalystExpr.canonicalized
          }
        }.getOrElse(Seq.empty).toSet
        splitConds.filter(e => !currConds.contains(e.canonicalized))
      }

      if (pushdownFilters.nonEmpty) {
        val pushCond = pushdownFilters.reduceLeft(And)
        for (oraExpression <- OraExpression.unapply(pushCond)) yield {
          val newFil = currQBlk.where.map(f =>
            OraBinaryOpExpression(AND, And(f.catalystExpr, pushdownCondition), f, oraExpression)
          ).getOrElse(oraExpression)

          currQBlk.copy(
            where = Some(newFil),
            catalystOp = Some(filOp),
            catalystProjectList = filOp.output
          )
        }
      } else {
        Some(currQBlk)
      }
    } else None
  }
}

case class JoinPushDown(inDSScan: DataSourceV2ScanRelation,
                        leftOraScan: OraScan,
                        leftQBlk: OraQueryBlock,
                        rightQBlk: OraQueryBlock,
                        pushdownCatalystOp: Join,
                        joinType: JoinType,
                        leftKeys: Seq[Expression],
                        rightKeys: Seq[Expression],
                        joinCond: Option[Expression],
                        sparkSession: SparkSession) extends OraPushdown {
  override val inOraScan: OraScan = leftOraScan
  override val inQBlk: OraQueryBlock = leftQBlk

  private[rules] def pushdownSQL: Option[OraQueryBlock] = None

}

case class SemiAntiJoinPushDown(inDSScan: DataSourceV2ScanRelation,
                                leftOraScan: OraScan,
                                leftQBlk: OraQueryBlock,
                                rightQBlk: OraQueryBlock,
                                pushdownCatalystOp: Join,
                                joinType: JoinType,
                                leftKeys: Seq[Expression],
                                rightKeys: Seq[Expression],
                                joinCond: Option[Expression],
                                sparkSession: SparkSession) extends OraPushdown {
  override val inOraScan: OraScan = leftOraScan
  override val inQBlk: OraQueryBlock = leftQBlk

  private[rules] def pushdownSQL: Option[OraQueryBlock] = None
}

case class ExpandPushDown(inDSScan: DataSourceV2ScanRelation,
                          inOraScan: OraScan,
                          inQBlk: OraQueryBlock,
                          pushdownCatalystOp: Expand,
                          sparkSession: SparkSession) extends OraPushdown {

  private[rules] def pushdownSQL: Option[OraQueryBlock] = None
}

case class AggregatePushDown(inDSScan: DataSourceV2ScanRelation,
                             inOraScan: OraScan,
                             inQBlk: OraQueryBlock,
                             pushdownCatalystOp: Aggregate,
                             sparkSession: SparkSession) extends OraPushdown {

  private[rules] def pushdownSQL: Option[OraQueryBlock] = None
}

