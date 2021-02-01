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

import org.apache.spark.sql.catalyst.{expressions => sparkexpr}
import org.apache.spark.sql.oracle.OraSparkUtils

trait ExprBuilders extends Arithmetics  { self : ExprTranslator =>

  import macroUniverse._

  object Literals {
    def unapply(t: mTree): Option[sparkexpr.Expression] =
      Option(t match {
        case Literal(Constant(v)) =>
          scala.util.Try(sparkexpr.Literal(v)).toOption.orNull
        case _ => null
      })
  }

  object Reference {

    private def exprInScope(nm : TermName) : Option[sparkexpr.Expression] =
      scope.get(nm).map(_.rhsExpr)

    def unapply(t: mTree): Option[sparkexpr.Expression] =
      Option(t match {
        case Ident(tNm : TermName) => exprInScope(tNm).orNull
        case tNm : TermName => exprInScope(tNm).orNull
        case _ => null
      })
  }

  object StaticValue {
    def unapply(t: mTree): Option[sparkexpr.Expression] =
      doWithWarning[sparkexpr.Expression](t,
      "evaluate to a static value",
      {
        val v = eval_tree(t)
        sparkexpr.Literal(v)
      })
  }

  object CatalystExpression {
    def unapply(t: mTree): Option[sparkexpr.Expression] =
      Option(t match {
        case Literals(e) => e
        case Reference(e) => e
        case BasicArith(e) => e
        case StaticValue(e) => e
        case _ => null
      })
  }

  object CatalystExpressions {
    def unapplySeq(tS: Seq[mTree]): Option[Seq[sparkexpr.Expression]] =
      OraSparkUtils.sequence(tS.map(CatalystExpression.unapply(_)))
  }

  implicit val toExpr = new Unliftable[sparkexpr.Expression] {
    def unapply(t: c.Tree): Option[sparkexpr.Expression] = CatalystExpression.unapply(t)
  }

}
