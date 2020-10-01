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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{And, AttributeSet, Expression, Literal}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.sources.Filter

abstract class OraExpression extends TreeNode[OraExpression] {

  def catalystExpr: Expression

  def genOraSQL(sqlBldr: StringBuilder, params: ArrayBuffer[Any]): Unit

  final override def verboseString(maxFields: Int): String = simpleString(maxFields)

  override def simpleStringWithNodeId(): String = {
    throw new UnsupportedOperationException(
      s"$nodeName does not implement simpleStringWithNodeId")
  }
}

object OraExpression {

  case class DummyOraExpression private (catalystExpr: Expression) extends OraExpression {
    def genOraSQL(sqlBldr: StringBuilder, params: ArrayBuffer[Any]): Unit = ()

    val children: Seq[OraExpression] = Seq.empty
  }

  case class DummyAndOraExpression private (
      left: OraExpression,
      right: OraExpression,
      catalystExpr: Expression)
      extends OraExpression {
    def genOraSQL(sqlBldr: StringBuilder, params: ArrayBuffer[Any]): Unit = ()

    val children: Seq[OraExpression] = Seq(left, right)
  }

  private def todoConvert(expr: Expression): OraExpression = DummyOraExpression(expr)

  private def todoConvert(exprs: Seq[Expression]): OraExpression = {
    val oExprs = exprs.map(DummyOraExpression)
    oExprs.tail.foldLeft(oExprs.head: OraExpression) {
      case (l, r) => DummyAndOraExpression(l, r, And(l.catalystExpr, r.catalystExpr))
    }
  }

  private def todoConvert(fil: Filter): OraExpression = DummyOraExpression(Literal(fil.toString))

  /*
   * Why pass in [[OraTable]]?
   * In general converting from catalyst Expression to OraExpression
   * we rely on catalyst Expression being resolved, which implies
   * we can assume an AttributeRef refers to a valid column in the
   * input OraPlan.
   *
   * This should be the case for Filter expressions also.
   * But providing the oracle scheme as a potential means to check.
   *
   * May remove this as wiring and constraints solidify.
   */
  def convert(fil: Filter, oraTab: OraTable): Option[OraExpression] = {
    // TODO
    None
  }

  def convert(expr: Expression, inputAttributeSet: AttributeSet): OraExpression = {
    todoConvert(expr)
  }

  def convert(exprs: Seq[Expression], inputAttributeSet: AttributeSet): OraExpression = {
    todoConvert(exprs)
  }
}
