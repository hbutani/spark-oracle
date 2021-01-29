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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.oracle.{OracleMetadata, OraNativeAggFuncInvoke, OraNativeRowFuncInvoke}

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
      case _ => null
    })

}
