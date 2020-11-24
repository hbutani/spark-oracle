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

import org.apache.spark.sql.catalyst.expressions.{Concat, Expression, StartsWith, StringTrim, StringTrimLeft, StringTrimRight, Substring, Upper}
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.sql.oracle.SQLSnippet.{comma, join, literalSnippet}

/**
 * Conversions for expressions in ''stringExpressions.scala''
 */

object Strings {

  case class OraStringTrim(catalystExpr: Expression,
                           trimType : SQLSnippet,
                           trimChar : Option[OraExpression],
                           trimSrc: OraExpression,
                          )
    extends OraExpression {

    val fnSnip = literalSnippet(TRIM)

    private def trimCharSnip = trimChar.map(_.orasql).getOrElse(SQLSnippet.empty)

    private def args = children.map(_.orasql)
    override def orasql: SQLSnippet =
      osql"$fnSnip(${trimType} ${trimCharSnip} FROM ${trimSrc})"

    override def children: Seq[OraExpression] = trimChar.toSeq ++ Seq(trimSrc)
  }

  private val TRIM_LEADING : SQLSnippet = literalSnippet(LEADING)
  private val TRIM_TRAILING : SQLSnippet = literalSnippet(TRAILING)
  private val TRIM_BOTH : SQLSnippet = literalSnippet(BOTH)

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case cE @ Substring(OraExpression(s), OraExpression(pos), OraExpression(len)) =>
        OraFnExpression(SUBSTR, cE, Seq(s, pos, len))
      case cE @ Concat(OraExpressions(oEs @ _*)) =>
        oEs.reduceLeft { (conE : OraExpression, oE : OraExpression) =>
          OraBinaryFnExpression(CONCAT, cE, conE, oE)
        }
      case cE @ StartsWith(OraExpression(left), sE@OraExpression(right)) =>
        OraBinaryOpExpression(LIKE, cE, left,
          OraBinaryFnExpression(CONCAT, sE, right, new OraLiteralSql("'%'"))
        )
      case cE@Upper(OraExpression(oE)) => OraUnaryFnExpression(UPPER, cE, oE)
      case cE@StringTrimLeft(OraExpression(trimSrc), None) =>
        OraStringTrim(cE, TRIM_LEADING, None, trimSrc)
      case cE@StringTrimLeft(OraExpression(trimSrc), OraExpression(trimChar)) =>
        OraStringTrim(cE, TRIM_LEADING, Some(trimChar), trimSrc)
      case cE@StringTrimRight(OraExpression(trimSrc), None) =>
        OraStringTrim(cE, TRIM_TRAILING, None, trimSrc)
      case cE@StringTrimRight(OraExpression(trimSrc), OraExpression(trimChar)) =>
        OraStringTrim(cE, TRIM_TRAILING, Some(trimChar), trimSrc)
      case cE@StringTrim(OraExpression(trimSrc), None) =>
        OraStringTrim(cE, TRIM_BOTH, None, trimSrc)
      case cE@StringTrim(OraExpression(trimSrc), OraExpression(trimChar)) =>
        OraStringTrim(cE, TRIM_BOTH, Some(trimChar), trimSrc)
      case _ => null
    })
}
