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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.oracle.{OraSQLImplicits, SQLSnippet, SQLSnippetProvider}
import org.apache.spark.sql.oracle.expressions.{OraExpression, OraExpressions}

case class OraJoinClause(joinType: JoinType, joinSrc: OraPlan, onCondition: OraExpression)
    extends SQLSnippetProvider {
  import OraSQLImplicits._

  lazy val joinTypeSQL: SQLSnippet = joinType match {
    case Inner => osql"join"
    case LeftOuter => osql"left outer join"
    case RightOuter => osql"right outer join"
    case FullOuter => osql"full outer join"
    case _ => null
  }

  private var joinAlias: Option[String] = None

  def setJoinAlias(a: String): Unit = {
    joinAlias = Some(a)
  }

  lazy val joinSrcSQL: SQLSnippet = {
    val srcSQL = joinSrc match {
      case ot: OraTableScan => SQLSnippet.tableQualId(ot.oraTable)
      case oQ: OraQueryBlock => SQLSnippet.subQuery(oQ.orasql)
    }

    val qualifier: SQLSnippet =
      joinAlias.map(jA => SQLSnippet.colRef(jA)).getOrElse(SQLSnippet.empty)

    srcSQL + qualifier

  }

  lazy val orasql: SQLSnippet = osql"${joinTypeSQL + joinSrcSQL} on ${onCondition.reifyLiterals}"
}

trait OraQueryBlock extends OraPlan with Product {

  import OraQueryBlock._

  def source: OraPlan
  def joins: Seq[OraJoinClause]
  def latJoin: Option[OraLateralJoin]
  def select: Seq[OraExpression]
  def where: Option[OraExpression]
  def groupBy: Option[Seq[OraExpression]]
  def catalystOp: Option[LogicalPlan]
  def catalystProjectList: Seq[NamedExpression]

  def canApply(plan: LogicalPlan): Boolean

  def getSourceAlias: Option[String] = getTagValue(ORA_SOURCE_ALIAS_TAG)
  def setSourceAlias(alias: String): Unit = {
    setTagValue(ORA_SOURCE_ALIAS_TAG, alias)
  }

  def copyBlock(
      source: OraPlan = source,
      joins: Seq[OraJoinClause] = joins,
      latJoin: Option[OraLateralJoin] = latJoin,
      select: Seq[OraExpression] = select,
      where: Option[OraExpression] = where,
      groupBy: Option[Seq[OraExpression]] = groupBy,
      catalystOp: Option[LogicalPlan] = catalystOp,
      catalystProjectList: Seq[NamedExpression] = catalystProjectList): OraQueryBlock

}

object OraQueryBlock {
  val ORA_SOURCE_ALIAS_TAG = TreeNodeTag[String]("_oraSourceAlias")

  /**
   * Start a new OraQueryBlock on top of the current block.
   * @return
   */
  def newBlockOnCurrent(currQBlock : OraQueryBlock) : OraQueryBlock = {
    val newOraExprs = OraExpressions.unapplySeq(currQBlock.catalystAttributes).get
    OraSingleQueryBlock(currQBlock, Seq.empty, None, newOraExprs,
      None, None, None, currQBlock.catalystAttributes)
  }
}
