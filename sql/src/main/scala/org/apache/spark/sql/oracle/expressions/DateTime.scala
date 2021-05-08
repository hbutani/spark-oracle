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

import org.apache.spark.sql.catalyst.expressions.{DateAdd, DateSub, DayOfMonth, Expression, Hour, Month, Year}
import org.apache.spark.sql.oracle.SQLSnippet

/**
 * Conversions for expressions in ''datetimeExpressions.scala''
 */
object DateTime {


  case class OraExtract(catalystExpr: Expression,
                        extractComponent : String,
                        child : OraExpression) extends OraExpression {
    import SQLSnippet._
    lazy val orasql: SQLSnippet =
      osql" extract(${literalSnippet(extractComponent)} from ${child}) "

    override def children: Seq[OraExpression] = Seq(child)
  }

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case cE@DateAdd(OraExpression(left), OraExpression(right)) =>
        OraBinaryOpExpression(PLUS, cE, left, right)
      case cE@DateSub(OraExpression(left), OraExpression(right)) =>
        OraBinaryOpExpression(MINUS, cE, left, right)
      case cE@Year(OraExpression(child)) => OraExtract(cE, "YEAR", child)
      case cE@Month(OraExpression(child)) => OraExtract(cE, "MONTH", child)
      case cE@DayOfMonth(OraExpression(child)) => OraExtract(cE, "DAY", child)
      case _ => null
    })
}
