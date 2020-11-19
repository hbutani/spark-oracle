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

import org.apache.spark.sql.catalyst.expressions.{Coalesce, Expression, IsNotNull, IsNull}
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.sql.oracle.expressions.Subquery.{OraNullCheckSubQuery, OraSubQuery}

/**
 * Conversions for expressions in ''nullExpressions.scala''
 */
object Nulls {

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case cE @ IsNull(OraExpression(child)) =>
        child match {
          case sq : OraSubQuery => OraNullCheckSubQuery(cE, SQLSnippet.NOT_EXISTS, sq)
          case _ => OraPostfixUnaryOpExpression(ISNULL, cE, child)
        }
      case cE @ IsNotNull(OraExpression(child)) =>
        child match {
          case sq : OraSubQuery => OraNullCheckSubQuery(cE, SQLSnippet.EXISTS, sq)
          case _ => OraPostfixUnaryOpExpression(ISNOTNULL, cE, child)
        }
      case ca @ Coalesce(OraExpressions(oEs @ _*)) =>
        OraFnExpression(COALESCE, ca, oEs)
      case _ => null
    })

}
