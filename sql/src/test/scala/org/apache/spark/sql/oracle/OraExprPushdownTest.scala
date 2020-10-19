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

package org.apache.spark.sql.oracle

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.connector.catalog.oracle.OraMetadataMgrInternalTest
import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.expressions.OraExpression

class OraExprPushdownTest extends AbstractTest with OraMetadataMgrInternalTest {

  lazy val parser = TestOracleHive.sparkSession.sessionState.sqlParser
  lazy val analyzer = TestOracleHive.sparkSession.sessionState.analyzer

  lazy val unit_test_table = TestOracleHive.sparkSession.table("unit_test")

  protected def resolveExpr(exprStr: String,
                            plan: LogicalPlan = unit_test_table.queryExecution.analyzed
                           ): Expression = {
    val expr = parser.parseExpression(exprStr)
    val r1 = analyzer.resolveExpressionBottomUp(expr, plan, false)

    // hack to resolve functions.
    val f1 = analyzer.ResolveFunctions(Filter(r1, plan))
    f1.asInstanceOf[Filter].condition
  }

  protected def resolveExpr(exprStr: String,
                            tNm: String): Expression = {
    resolveExpr(exprStr,
      TestOracleHive.sparkSession.table(tNm).queryExecution.analyzed
    )
  }

  protected def oraExpression(expr: Expression) =
    OraExpression.unapply(expr)

  override def beforeAll(): Unit = {
    super.beforeAll()
    TestOracleHive.sql("use oracle.sparktest")
  }

  def test(nm: String,
           exprStr: String,
           plan: () => LogicalPlan): Unit = test(nm) { td =>
    val expr = resolveExpr(exprStr, plan())
    expr match {
      case OraExpression(oraExpr) => ()
      case _ =>
        throw new AnalysisException(
          s"""for
             |  ${exprStr}
             |no oracle expression
             |spark expression: ${expr.toString()}""".stripMargin
        )
    }
  }

  val pushableExpressions = Seq(
    """
      |c_int > 1 and
      |    c_date is null and
      |    c_timestamp is not null
      |""".stripMargin,
    "(c_int % 5) < (c_int * 5)",
    "abs(c_long) > c_long",
    """
      |case
      |  when c_short > 0 then "positive"
      |  when c_short < 0 then "negative"
      |  else "zero"
      |end in ("positive", "zero")""".stripMargin,
    """
      |c_int > 1 and
      |    (c_int % 5) < (c_int * 5) and
      |    abs(c_long) > c_long and
      |    c_date is null and
      |    c_timestamp is not null
      |""".stripMargin,
    """
      |c_int > 1 and
      |    case
      |       when c_short > 0 then "positive"
      |       when c_short < 0 then "negative"
      |       else "zero"
      |     end in ("positive", "zero") and
      |    (c_int % 5) < (c_int * 5) and
      |    abs(c_long) > c_long and
      |    c_date is null and
      |    c_timestamp is not null""".stripMargin
  )

  for ((pE, i) <- pushableExpressions.zipWithIndex) {
    test(s"test_pushExpr_${i}", pE, () => unit_test_table.queryExecution.analyzed)
  }
}
