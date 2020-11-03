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
package org.apache.spark.sql.oracle.operators

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.sql.oracle.expressions.{OraExpression, OraExpressions}
import org.apache.spark.sql.oracle.expressions.Named.OraColumnRef

case class OraJoinClause(joinType: JoinType, joinSrc: OraPlan, onClause: OraExpression)

trait OraQueryBlockState { self: OraQueryBlock =>

  lazy val hasComputedShape: Boolean = select.exists(o => !o.isInstanceOf[OraColumnRef])
  lazy val hasOuterJoin: Boolean =
    joins.exists(j => Set[JoinType](LeftOuter, RightOuter, FullOuter).contains(j.joinType))
  lazy val hasFilter: Boolean = where.isDefined
  lazy val hasAggregate : Boolean = groupBy.isDefined

  def canApply(plan: LogicalPlan): Boolean = plan match {
    case p: Project => true
    case f: Filter => !(hasOuterJoin || hasAggregate)
    case j@Join(_, _, (LeftOuter | RightOuter | FullOuter), _, _) =>
      !(hasComputedShape || hasFilter)
    case j: Join => !hasComputedShape
    case e: Expand => !hasComputedShape
    case a: Aggregate => !hasComputedShape
  }

}

trait OraQueryBlockSQLSnippets {self: OraQueryBlock =>

  private lazy val sourceSnippet : SQLSnippet = source match {
    case ot : OraTableScan => SQLSnippet.tableQualId(ot.oraTable)
    case oQ : OraQueryBlock => SQLSnippet.subQuery(oQ.orasql)
  }

  lazy val selectListSQL = select.map(_.orasql)

  lazy val sourcesSQL : SQLSnippet = {
    sourceSnippet
    // TODO joins
  }

  lazy val whereConditionSQL = where.map(_.orasql)

  lazy val groupByListSQL = groupBy.map(_.map(_.orasql))
}

/**
 * Represents a Oracle SQL query block.
 *
 * @param source  the initial [[OraPlan]] on which this QueryBlock is layered.
 * @param joins   the `inner` or `outer` joins in this query block.
 * @param select  the projected expressions of this query block.
 * @param where   an optional filter expression
 * @param groupBy optional aggregation expressions.
 */
case class OraQueryBlock(source: OraPlan,
                         joins: Seq[OraJoinClause],
                         select: Seq[OraExpression],
                         where: Option[OraExpression],
                         groupBy: Option[Seq[OraExpression]],
                         catalystOp: Option[LogicalPlan],
                         catalystProjectList: Seq[NamedExpression])
  extends OraPlan with OraQueryBlockState with OraQueryBlockSQLSnippets {

  val children: Seq[OraPlan] = Seq(source)

  override def orasql: SQLSnippet = {
    SQLSnippet.select(selectListSQL : _*).
      from(sourcesSQL).
      where(whereConditionSQL).
      groupBy(groupByListSQL)
  }

  /**
   * Start a new OraQueryBlock on top of the current block.
   * @return
   */
  def newBlockOnCurrent: OraQueryBlock = {
    val newOraExprs = OraExpressions.unapplySeq(catalystAttributes).get
    OraQueryBlock(this, Seq.empty, newOraExprs, None, None, None, catalystAttributes)
  }
}
