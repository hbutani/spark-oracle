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

package org.apache.spark.sql.oracle.expressions

import org.apache.spark.sql.catalyst.expressions.{And, AttributeSet, Expression, Literal}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.oracle.{OraSparkUtils, OraSQLImplicits, SQLSnippet}
import org.apache.spark.sql.sources.Filter

abstract class OraExpression extends TreeNode[OraExpression] with OraSQLImplicits {

  def catalystExpr: Expression

  def orasql: SQLSnippet

  final override def verboseString(maxFields: Int): String = simpleString(maxFields)

  override def simpleStringWithNodeId(): String = {
    throw new UnsupportedOperationException(
      s"$nodeName does not implement simpleStringWithNodeId")
  }
}

trait OraLeafExpression { self: OraExpression =>
  val children: Seq[OraExpression] = Seq.empty
}

case class OraUnaryOpExpression(op: String, catalystExpr: Expression, child: OraExpression)
    extends OraExpression {
  val children: Seq[OraExpression] = Seq(child)
  override def orasql: SQLSnippet = SQLSnippet.unaryOp(op, child.orasql)
}

case class OraPostfixUnaryOpExpression(op: String, catalystExpr: Expression, child: OraExpression)
    extends OraExpression {
  val children: Seq[OraExpression] = Seq(child)
  override def orasql: SQLSnippet = SQLSnippet.postfixUnaryOp(op, child.orasql)
}

case class OraUnaryFnExpression(fn: String, catalystExpr: Expression, child: OraExpression)
    extends OraExpression {
  val children: Seq[OraExpression] = Seq(child)
  override def orasql: SQLSnippet = SQLSnippet.call(fn, child.orasql)
}

case class OraBinaryOpExpression(
    op: String,
    catalystExpr: Expression,
    left: OraExpression,
    right: OraExpression)
    extends OraExpression {
  val children: Seq[OraExpression] = Seq(left, right)
  override def orasql: SQLSnippet = SQLSnippet.operator(op, left.orasql, right.orasql)
}

case class OraBinaryFnExpression(
    fn: String,
    catalystExpr: Expression,
    left: OraExpression,
    right: OraExpression)
    extends OraExpression {
  val children: Seq[OraExpression] = Seq(left, right)
  override def orasql: SQLSnippet = SQLSnippet.call(fn, left.orasql, right.orasql)
}

case class OraFnExpression(fn: String, catalystExpr: Expression, children: Seq[OraExpression])
    extends OraExpression {
  val cSnips = children.map(_.orasql)
  override def orasql: SQLSnippet = SQLSnippet.call(fn, cSnips: _*)
}

object OraExpression {

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case OraLiterals(oE) => oE
      case Arithmetic(oE) => oE
      case Conditional(oE) => oE
      case Named(oE) => oE
      case Predicates(oE) => oE
      case Nulls(oE) => oE
      case _ => null
    })

  /*
   * Why pass in [[OraTable]]?
   * In general converting from catalyst Expression to OraExpression
   * we rely on catalyst Expression being resolved, which implies
   * we can assume an AttributeRef refers to a valid column in the
   * input OraPlan.
   *
   * This should be the case for Filter expressions also.
   * But providing the oracle scheme as a potential means to check.
   *
   * May remove this as wiring and constraints solidify.
   */
  def convert(fil: Filter, oraTab: OraTable): Option[OraExpression] = {
    // TODO
    None
  }

  def convert(expr: Expression, inputAttributeSet: AttributeSet): Option[OraExpression] = {
    unapply(expr)
  }

  def convert(exprs: Seq[Expression], inputAttributeSet: AttributeSet): Option[OraExpression] = {
    val oEs = exprs.map(convert(_, inputAttributeSet)).collect {
      case Some(oE) => oE
    }

    if (oEs.nonEmpty) {
      Some(oEs.tail.foldLeft(oEs.head) {
        case (left, right) => OraBinaryOpExpression(AND, left.catalystExpr, left, right)
      })
    } else {
      None
    }
  }
}

object OraExpressions {
  def unapplySeq(eS: Seq[Expression]): Option[Seq[OraExpression]] =
    OraSparkUtils.sequence(eS.map(OraExpression.unapply(_)))

}
