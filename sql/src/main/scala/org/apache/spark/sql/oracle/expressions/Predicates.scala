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

import org.apache.spark.sql.catalyst.expressions.{And, BinaryComparison, EqualNullSafe, Expression, In, InSet, InSubquery, Literal, Not, Or, Predicate}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.sql.oracle.operators.OraQueryBlock

/**
 * Conversions for expressions in ''predicates.scala''
 */
object Predicates {

  case class OraIn(catalystExpr: Predicate, oExpr: OraExpression, inList: Seq[OraExpression])
      extends OraExpression {

    import SQLSnippet._

    override def orasql: SQLSnippet =
      oExpr.orasql + IN + LPAREN + csv(inList.map(_.orasql): _*) + RPAREN

    override def children: Seq[OraExpression] = oExpr +: inList
  }

  /**
   * Represents a subQuery check (such as IN or NOT IN)
   * This is the Oracle SQL for a Spark [[LeftSemi]] or [[LeftAnti]]
   * `join` operattion.
   *
   * @param joinOp
   * @param joiningExprs
   * @param op
   * @param qryBlk
   */
  case class OraSubQueryFilter(joinOp : Join,
                               joiningExprs : Seq[OraExpression],
                               op : SQLSnippet,
                               qryBlk : OraQueryBlock) extends OraExpression {
    override def catalystExpr: Expression = joinOp.condition.get

    override def orasql: SQLSnippet = {
      val joinExprsSQL : Seq[SQLSnippet] = joiningExprs.map(_.orasql)
      val subQrySQL = qryBlk.orasql

      if (joinExprsSQL.size > 1) {
        osql" (${SQLSnippet.csv(joinExprsSQL : _*)}) ${op} ( ${subQrySQL} )"
      } else {
        osql" ${joinExprsSQL.head} ${op} ( ${subQrySQL} )"
      }
    }

    override def children: Seq[OraExpression] = joiningExprs
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
            Seq(
              left,
              right,
              OraLiteral(Literal(0)).toLiteralSql,
              OraLiteral(Literal(1)).toLiteralSql)),
          OraLiteral(Literal(0)).toLiteralSql)
      case cE @ BinaryComparison(OraExpression(left), OraExpression(right)) =>
        OraBinaryOpExpression(cE.symbol, cE, left, right)
      case _ => null
    })
}
