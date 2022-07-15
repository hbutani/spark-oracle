/*
  Copyright (c) 2021, Oracle and/or its affiliates.

  This software is dual-licensed to you under the Universal Permissive License
  (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
  2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
  either license.

  If you elect to accept the software under the Apache License, Version 2.0,
  the following applies:

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package org.apache.spark.sql.oracle.expressions

import org.apache.spark.sql.catalyst.expressions.{ApplyFunctionExpression, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, V2Aggregator}
import org.apache.spark.sql.connector.catalog.oracle._

object OraNativeFunctions {

  /**
   * Handle any vagaries of oracle native function invocations here.
   * - only one so far is generate `USER` instead of `USER()`
   *
   * @param fnDef
   * @param cE
   * @param childOEs
   * @return
   */
  private def oraFnInvokeExpr(fnDef : OracleMetadata.OraFuncDef,
                              cE : Expression,
                             childOEs: Seq[OraExpression]) : OraExpression = {
    if (fnDef.owner == "SYS" && fnDef.name == "USER") {
      new OraLiteralSql("USER")
    } else {
      OraFnExpression(fnDef.orasql_fnname, cE, childOEs)
    }
  }

  def unapply(e: Expression): Option[OraExpression] =
    Option(e match {
      case cE@OraNativeRowFuncInvoke(fnDef, _, OraExpressions(oEs @ _*)) =>
        oraFnInvokeExpr(fnDef, cE, oEs)
      case cE@OraNativeAggFuncInvoke(fnDef, _, OraExpressions(oEs @ _*)) =>
        oraFnInvokeExpr(fnDef, cE, oEs)
      case cE@ApplyFunctionExpression(oraFunc : OraScalarFunction, OraExpressions(oEs @ _*)) =>
        oraFnInvokeExpr(oraFunc.fnDef, cE, oEs)
      case cE@AggregateExpression(
      V2Aggregator(aggFunc : OraAggregateFunction, OraExpressions(oEs @ _*), _, _),
      Complete, _, _, _) =>
        oraFnInvokeExpr(aggFunc.fnDef, cE, oEs)
      case _ => null
    })

}
