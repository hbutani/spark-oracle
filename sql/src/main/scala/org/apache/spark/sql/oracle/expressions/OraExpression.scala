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

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.oracle.{OraSparkUtils, OraSQLImplicits, SQLSnippet, SQLSnippetProvider}
import org.apache.spark.sql.sources.Filter

abstract class OraExpression extends TreeNode[OraExpression]
  with OraSQLImplicits with SQLSnippetProvider {

  def catalystExpr: Expression

  def orasql: SQLSnippet

  def reifyLiterals : OraExpression = this transformUp {
    case l : OraLiteral => l.toLiteralSql
  }

  /*
   * When showing `OraExpression` such as in the treeString of
   * `OraPlan` just show the wrapped `catalystExpr`
   */
  override def stringArgs: Iterator[Any] = Iterator(catalystExpr)

  /*
   * Copy behavior for String and treeString from [[Expression]]
   * `treeString` of a OraPlan will show `OraExpression` similar
   * to how spark `Expression` are shown in treeString of
   * spark `QueryPlan`.
   */

  def prettyName: String = nodeName.toLowerCase(Locale.ROOT)

  protected def flatArguments: Iterator[Any] = stringArgs.flatMap {
    case t: Iterable[_] => t
    case single => single :: Nil
  }

  // Marks this as final, Expression.verboseString should never be called, and thus shouldn't be
  // overridden by concrete classes.
  final override def verboseString(maxFields: Int): String = simpleString(maxFields)

  override def simpleString(maxFields: Int): String = toString

  override def toString: String = prettyName + truncatedString(
    flatArguments.toSeq, "(", ", ", ")", SQLConf.get.maxToStringFields)

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
      case Subquery(oE) => oE
      case Casts(oE) => oE
      case IgnoreExpressions(oE) => oE
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

  def convert(expr: Expression): Option[OraExpression] = {
    unapply(expr)
  }

  def convert(exprs: Seq[Expression]): Option[OraExpression] = {
    val oEs = exprs.map(convert(_)).collect {
      case Some(oE) => oE
    }

    if (oEs.nonEmpty) {
      Some(oEs.tail.foldLeft(oEs.head) {
        case (left, right) =>
          OraBinaryOpExpression(AND, And(left.catalystExpr, right.catalystExpr), left, right)
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
