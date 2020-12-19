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

import org.apache.spark.sql.catalyst.expressions.{Expression, NullOrdering, SortDirection, SortOrder}
import org.apache.spark.sql.oracle.SQLSnippet

object Sorts {

  case class OraSortOrder(catalystExpr : SortOrder,
                          child : OraExpression,
                          direction : SortDirection,
                          nullOrdering: NullOrdering
                         ) extends OraExpression {

    val children: Seq[OraExpression] = Seq(child)

    private val  sortDirSnip = SQLSnippet.literalSnippet(direction.sql)
    private val nullOrderSnip = SQLSnippet.literalSnippet(nullOrdering.sql)

    override def orasql: SQLSnippet = osql"${child} ${sortDirSnip} ${nullOrderSnip}"
  }

  def unapply(e: Expression) : Option[OraExpression] = Option(e match {
    case cE@SortOrder(OraExpression(oE), direction, nullOrdering, sameOrderExpressions)
    if sameOrderExpressions.isEmpty =>
      OraSortOrder(cE, oE, direction, nullOrdering)
    case _ => null
  })
}