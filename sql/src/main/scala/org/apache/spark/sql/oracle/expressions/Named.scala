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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.oracle.{SQLSnippet, SQLSnippetProvider}

/**
 * Conversions for expressions in ''namedExpressions.scala''
 */
object Named {

  sealed trait OraFixedColNm  extends SQLSnippetProvider {
    def nm : String
  }
  case class QualFixedColNm(src : String, nm : String) extends OraFixedColNm {
    def orasql : SQLSnippet = SQLSnippet.qualifiedColRef(src, nm)
  }
  case class UnQualFixedColNm(nm : String) extends OraFixedColNm {
    def orasql : SQLSnippet = SQLSnippet.colRef(nm)
  }

  /**
   * [[OraNamedExpression]] expressions can have a name in oracle-sql
   * that is different from the [[AttributeReference spark attribute]]
   * they encapsulate. This is because of: difference in case handling
   * behavior between Spark and Oracle, or because when dealing
   * with [[Logical spark optimize logical plans]]
   * [[AttributeReference attributes]] are identified by [[ExprId]]
   * and not by nameIso you may have multiple attrs with the same name
   * in an opeartor which need to be disambiguated in the generated
   * oracle-sql). See [[OraFixColumnNames]] for more details.
   *  - [[OraNamedExpression]] can have an optionally associated `_aliasInOraSQL`
   *    and [[OraColumnRef]] additionally can have an optional `_nmInOraSQL`
   *  - These are used to generate column references in oracle-sql. The
   *    `_nmInOraSQL` can be [[QualFixedColNm qualified]] or
   *    [[UnQualFixedColNm not]]; if present it is used as the column name in
   *    oracle-sql. If present the `_aliasInOraSQL` is applied as the column
   *    alias in oracle-sql.
   */
  trait OraNamedExpression extends OraExpression {
    override def catalystExpr: NamedExpression

    protected var _aliasInOraSQL : Option[String] = None
    def getOraFixedAlias : Option[String] = _aliasInOraSQL
    def setOraFixedAlias(alias : String) : Unit = {
      _aliasInOraSQL = Some(alias)
    }

    def outNmInOraSQL : String
  }

  case class OraAlias(catalystExpr: Alias, child: OraExpression) extends OraNamedExpression {
    def orasql: SQLSnippet = {
      child.orasql.as(outNmInOraSQL)
    }
    lazy val children: Seq[OraExpression] = Seq(child)

    def outNmInOraSQL : String = _aliasInOraSQL.getOrElse(catalystExpr.name)
  }

  case class OraColumnRef(catalystExpr: AttributeReference)
      extends OraNamedExpression
      with OraLeafExpression {

    private var _nmInOraSQL : Option[OraFixedColNm] = None

    def getOraFixedNm : Option[OraFixedColNm] = _nmInOraSQL
    def setOraFixedNm(nm : OraFixedColNm) : Unit = {
      _nmInOraSQL = Some(nm)
    }

    def orasql: SQLSnippet = {
      var sqlSnip = getOraFixedNm.map(_.orasql).getOrElse(SQLSnippet.colRef(catalystExpr.name))
      if (_aliasInOraSQL.isDefined) {
        sqlSnip = sqlSnip.as(_aliasInOraSQL.get)
      }
      sqlSnip
    }

    def outNmInOraSQL : String =
      _aliasInOraSQL.getOrElse(_nmInOraSQL.map(_.nm).getOrElse(catalystExpr.name))

  }

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case cE @ Alias(OraExpression(child), _) => OraAlias(cE, child)
      case cE: AttributeReference => OraColumnRef(cE)
      case _ => null
    })

}
