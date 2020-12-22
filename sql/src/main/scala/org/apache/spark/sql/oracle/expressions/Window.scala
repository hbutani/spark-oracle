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

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, WindowExpression, WindowFrame, WindowSpecDefinition}
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.sql.oracle.SQLSnippet.{ORDER_BY, PARTITION_BY}
import org.apache.spark.sql.oracle.SQLSnippet.comma.windowOverAttr
import org.apache.spark.sql.types.{IntegralType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import scala.:+


object Window {


  case class OraWindowSpec(partitionSpec: Option[Seq[OraExpression]],
                           orderSpec: Option[Seq[OraExpression]],
                           frameSpecification: WindowFrame) extends OraExpression {

    val expr = partitionSpec.map(_.map(_.catalystExpr)) ++ orderSpec.map(_.map(_.catalystExpr))
    // TODO have to Rectify. This is Junk.
    override def catalystExpr: Expression = expr.flatten.head

    lazy val part = partitionSpec.map(_.map(_.reifyLiterals.orasql))
    lazy val pf = windowOverAttr(part)

    lazy val order = orderSpec.map(_.map(_.reifyLiterals.orasql))
    lazy val of = windowOverAttr(order)

    lazy val frame = frameSpecification.map(_.sql).head
    lazy val framexpr = OraLiteralSql(Literal(UTF8String.fromString(frame), StringType))

    override def orasql: SQLSnippet = {
      (part.get.isEmpty, order.get.isEmpty) match {
        case (false, false) => osql"($PARTITION_BY $pf $ORDER_BY $of $framexpr)"
        case (true, false) => osql"($ORDER_BY $of $framexpr)"
        case (false, true) => osql"($PARTITION_BY $pf)"
        case (true, true) => osql""
      }
    }
    override def children: Seq[OraExpression] = partitionSpec.get ++ orderSpec.get
  }

  case class OraWindow(windowFunction: OraExpression,
                       windowSpec: OraExpression) extends OraExpression {
    override def catalystExpr: Expression = windowFunction.catalystExpr
    lazy val func = windowFunction.orasql
    lazy val spec = windowSpec.orasql
    override def orasql: SQLSnippet = osql"$func OVER $spec"
    override def children: Seq[OraExpression] = Seq(windowFunction, windowSpec)
  }

  def unapply(e : Expression) : Option[OraExpression] = Option(x = e match {
    case oE@WindowExpression(windowFunction, windowSpec) =>
      (OraExpression.unapply(windowFunction), OraExpression.unapply(windowSpec)) match {
        case (Some(a), Some(b)) => OraWindow(a, b)
        case _ => null
      }
    case wE@WindowSpecDefinition(partitionSpec, orderSpec, frameSpecification) =>
      OraWindowSpec(OraExpressions.unapplySeq(partitionSpec),
        OraExpressions.unapplySeq(orderSpec), frameSpecification)
    case _ => null
  })
}
