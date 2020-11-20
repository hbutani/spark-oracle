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

import org.apache.spark.sql.catalyst.expressions.{Concat, Expression, StartsWith, Substring}

/**
 * Conversions for expressions in ''stringExpressions.scala''
 */

object Strings {
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
      case _ => null
    })
}
