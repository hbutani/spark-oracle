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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression}
import org.apache.spark.sql.oracle.SQLSnippet

/**
 * Conversions for expressions in ''namedExpressions.scala''
 */
object Named {

  case class OraAlias(catalystExpr: Alias, child: OraExpression) extends OraExpression {
    lazy val orasql: SQLSnippet = child.orasql.as(catalystExpr.name)
    override def children: Seq[OraExpression] = Seq(child)
  }

  case class OraColumnRef(catalystExpr: AttributeReference)
      extends OraExpression
      with OraLeafExpression {
    lazy val orasql: SQLSnippet = SQLSnippet.colRef(catalystExpr.name)
  }

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case cE @ Alias(OraExpression(child), _) => OraAlias(cE, child)
      case cE: AttributeReference => OraColumnRef(cE)
      case _ => null
    })
}
