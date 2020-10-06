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

import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.{
  And,
  BinaryComparison,
  EqualNullSafe,
  Expression,
  In,
  InSet,
  InSubquery,
  Literal,
  Not,
  Or,
  Predicate
}
import org.apache.spark.sql.oracle.SQLSnippet

/**
 * Conversions for expressions in ''predicates.scala''
 */
object Predicates {

  case class OraIn(catalystExpr: Predicate, oExpr: OraExpression, inList: Seq[OraExpression])
      extends OraExpression {

    import SQLSnippet._

    override def orasql: SQLSnippet =
      oExpr.orasql + IN + LPAREN ++ inList.map(_.orasql) + RPAREN

    override def children: Seq[OraExpression] = oExpr +: inList
  }

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case cE @ Not(OraExpression(oE)) => OraUnaryOpExpression(NOT, cE, oE)
      case cE @ InSubquery(OraExpressions(oEs @ _*), query) =>
        ??? // TODO
      case cE @ In(OraExpression(value), OraExpressions(oEs @ _*)) =>
        OraIn(cE, value, oEs)
      case cE @ InSet(OraExpression(value), hset) =>
        /*
         * For now try to convert the inList to a Literal list and then a
         * OE list
         */
        Try {
          val lits: Seq[Expression] = hset.map(Literal.fromObject(_): Expression).toSeq
          OraExpressions
            .unapplySeq(lits)
            .map { inList =>
              OraIn(cE, value, inList)
            }
            .orNull
        }.getOrElse(null)
      case cE @ And(OraExpression(left), OraExpression(right)) =>
        OraBinaryOpExpression(AND, cE, left, right)
      case cE @ Or(OraExpression(left), OraExpression(right)) =>
        OraBinaryOpExpression(OR, cE, left, right)
      case cE @ EqualNullSafe(OraExpression(left), OraExpression(right)) =>
        OraBinaryOpExpression(
          EQ,
          cE,
          OraFnExpression(
            DECODE,
            cE,
            Seq(left, right, OraLiteralSql(Literal(0)), OraLiteralSql(Literal(1)))),
          OraLiteralSql(Literal(0)))
      case cE @ BinaryComparison(OraExpression(left), OraExpression(right)) =>
        OraBinaryOpExpression(cE.symbol, cE, left, right)
      case _ => null
    })
}
