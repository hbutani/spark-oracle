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

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.oracle.{OraSQLImplicits, SQLSnippet, SQLSnippetProvider}
import org.apache.spark.sql.oracle.expressions.{OraExpression, OraExpressions}
import org.apache.spark.sql.oracle.expressions.Named.OraColumnRef

case class OraJoinClause(joinType: JoinType,
                         joinSrc: OraPlan,
                         onCondition: OraExpression
                        ) extends SQLSnippetProvider {
  import OraSQLImplicits._

  lazy val joinTypeSQL : SQLSnippet = joinType match {
    case Inner => osql"join"
    case LeftOuter => osql"left outer join"
    case RightOuter => osql"right outer join"
    case FullOuter => osql"full outer join"
    case _ => null
  }

  private var joinAlias : Option[String] = None

  def setJoinAlias(a : String) : Unit = {
    joinAlias = Some(a)
  }

  lazy val joinSrcSQL : SQLSnippet = {
    val srcSQL = joinSrc match {
      case ot : OraTableScan => SQLSnippet.tableQualId(ot.oraTable)
      case oQ : OraQueryBlock => SQLSnippet.subQuery(oQ.orasql)
    }

    val qualifier : SQLSnippet =
      joinAlias.map(jA => SQLSnippet.colRef(jA)).getOrElse(SQLSnippet.empty)

    srcSQL + qualifier

  }

  lazy val orasql: SQLSnippet = osql"${joinTypeSQL + joinSrcSQL} on ${onCondition.reifyLiterals}"
}

trait OraQueryBlockState { self: OraQueryBlock =>

  lazy val hasComputedShape: Boolean =
    select.exists(o => !o.isInstanceOf[OraColumnRef])
  lazy val hasOuterJoin: Boolean =
    joins.exists(j => Set[JoinType](LeftOuter, RightOuter, FullOuter).contains(j.joinType))
  lazy val hasFilter: Boolean = where.isDefined
  private lazy val hasAggregations : Boolean = {
    select.map {oE =>
      oE.map(_.catalystExpr).collect {
        case aE : AggregateExpression => aE
      }
    }.flatten.nonEmpty
  }
  lazy val hasAggregate : Boolean = {
    groupBy.isDefined || hasAggregations
  }

  lazy val hasJoins = joins.nonEmpty
  lazy val hasLatJoin = latJoin.isDefined

  def canApply(plan: LogicalPlan): Boolean = plan match {
    case p: Project => true
    case f: Filter => !(hasOuterJoin || hasAggregate)
    case j@Join(_, _, (LeftOuter | RightOuter | FullOuter), _, _) =>
      !(hasComputedShape || hasFilter || hasAggregate || hasLatJoin)
    case j: Join => !(hasComputedShape || hasAggregate || hasLatJoin)
    case e: Expand => !(hasComputedShape || hasAggregate || hasLatJoin)
    case a: Aggregate => !(hasComputedShape || hasAggregate)
    case gl : GlobalLimit => !(hasOuterJoin || hasAggregate)
  }

}

trait OraQueryBlockSQLSnippets {self: OraQueryBlock =>

  import OraSQLImplicits._

  private var sourceAlias : Option[String] = None

  def setSourceAlias(a : String) : Unit = {
    sourceAlias = Some(a)
  }

  private def sourceSnippet : SQLSnippet = {
    val srcSQL = source match {
      case ot : OraTableScan => SQLSnippet.tableQualId(ot.oraTable)
      case oQ : OraQueryBlock => SQLSnippet.subQuery(oQ.orasql)
    }
    val qualifier : SQLSnippet =
      sourceAlias.map(sA => SQLSnippet.colRef(sA)).getOrElse(SQLSnippet.empty)
    srcSQL + qualifier
  }

  protected def selectListSQL = select.map(_.reifyLiterals.orasql)

  def sourcesSQL : SQLSnippet = {
    var ss = sourceSnippet ++ joins.map(_.orasql)
    if (latJoin.isDefined) {
      ss = ss + osql" , lateral ( ${latJoin.get.orasql} )"
    }
    ss
  }

  protected def whereConditionSQL = where.map(_.orasql)

  protected def groupByListSQL = groupBy.map(_.map(_.reifyLiterals.orasql))
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
                         latJoin : Option[OraLateralJoin],
                         select: Seq[OraExpression],
                         where: Option[OraExpression],
                         groupBy: Option[Seq[OraExpression]],
                         catalystOp: Option[LogicalPlan],
                         catalystProjectList: Seq[NamedExpression])
  extends OraPlan with OraQueryBlockState with OraQueryBlockSQLSnippets {

  val children: Seq[OraPlan] = Seq(source) ++ joins.map(_.joinSrc)

  override def stringArgs: Iterator[Any] = Iterator(catalystProjectList, select, where, groupBy)

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
    OraQueryBlock(this, Seq.empty, None, newOraExprs, None, None, None, catalystAttributes)
  }

}
