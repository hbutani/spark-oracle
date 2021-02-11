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

package org.apache.spark.sql.sqlmacros

import scala.language.experimental.macros
import scala.reflect.macros.blackbox._

import org.apache.spark.sql.catalyst.{expressions => sparkexpr}

class SQLMacro(val c : Context) extends ExprTranslator {

  def buildExpression(params : Seq[mTree],
                      stats : Seq[mTree]) : Option[sparkexpr.Expression] = {

    try {

      for ((p, i) <- params.zipWithIndex) {
        translateParam(i, p)
      }

      for (s <- stats.init) {
        translateStat(s)
      }

      Some(translateExprTree(stats.last)).map(optimizeExpr)

    } catch {
      case MacroTransException => None
      case e : Throwable => throw e
    }

  }

  def udm1_impl[RT : c.WeakTypeTag, A1 : c.WeakTypeTag](f : c.Expr[Function1[A1, RT]])
  : c.Expr[Either[Function1[A1, RT], SQLMacroExpressionBuilder]] = {

    import macroUniverse._

    val (params, stats) = extractFuncParamsStats(f.tree)

    val expr = buildExpression(params, stats)

    expr match {
      case Some(e) =>
        val eSer = SQLMacroExpressionBuilder.serialize(e)
        c.Expr[Either[Function1[A1, RT], SQLMacroExpressionBuilder]](
          q"scala.util.Right(org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder(${eSer}))")
      case None =>
        c.Expr[Either[Function1[A1, RT], SQLMacroExpressionBuilder]](
        q"scala.util.Left(${f.tree})")
    }
  }

}
