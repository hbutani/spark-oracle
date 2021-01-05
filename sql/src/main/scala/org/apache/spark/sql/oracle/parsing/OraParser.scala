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
package org.apache.spark.sql.oracle.parsing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.oracle.OraSparkUtils
import org.apache.spark.sql.oracle.commands.{ExplainPushdown, SparkOraVersion}
import org.apache.spark.sql.oracle.rules.OraLogicalRules
import org.apache.spark.sql.types.{DataType, StructType}

class OraParser(baseParser : ParserInterface) extends ParserInterface {

  /**
   * Ensure that the [[OraSQLPushdownRule]] is
   * applied as an `extraOptimizations`. This ensures that the
   * [[OraSQLPushdownRule]] is applied in the *User Provided Optimizers*
   * **Batch** of the [[org.apache.spark.sql.execution.SparkOptimizer]],
   * which comes after all built-in rewrites. The rewrites in
   * [[OraSQLPushdownRule]] assume the input [[LogicalPlan]] has been
   * transformed by all Spark rewrite rules.
   * @param ss
   */
  private def ensurePushdownRuleRegistered(ss : SparkSession) : Unit = {
    val extraRules = ss.sessionState.experimentalMethods.extraOptimizations

    if (!extraRules.contains(OraLogicalRules)) {
      ss.sessionState.experimentalMethods.extraOptimizations =
        ss.sessionState.experimentalMethods.extraOptimizations :+ OraLogicalRules
    }
  }

  val sparkOraExtensionsParser = new SparkOraExtensionsParser(baseParser)

  override def parsePlan(sqlText: String): LogicalPlan = {
    ensurePushdownRuleRegistered(OraSparkUtils.currentSparkSession)

    val extensionsPlan = sparkOraExtensionsParser.parseExtensions(sqlText)

    if (extensionsPlan.successful ) {
      extensionsPlan.get
    } else {
      try {
        baseParser.parsePlan(sqlText)
      } catch {
        case pe : ParseException =>
          val splFailureDetails =
            extensionsPlan.asInstanceOf[SparkOraExtensionsParser#NoSuccess].msg
          throw new ParseException(pe.command,
            s"""${pe.message}
               |[
               |Also failed to parse as spark-ora extensions:
               | ${splFailureDetails}
               |]""".stripMargin,
            pe.start,
            pe.stop
          )
      }
    }
  }

  def parseExpression(sqlText: String): Expression =
    baseParser.parseExpression(sqlText)

  def parseTableIdentifier(sqlText: String): TableIdentifier =
    baseParser.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    baseParser.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
  baseParser.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    baseParser.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    baseParser.parseDataType(sqlText)

}


private[parsing] class SparkOraExtensionsParser(val baseParser : ParserInterface)
  extends AbstractLightWeightSQLParser {

  private def sparkSession = SparkSession.getActiveSession.get

  protected val SPARK_ORA = Keyword("SPARK_ORA")
  protected val VERSION = Keyword("VERSION")
  protected val EXPLAIN = Keyword("EXPLAIN")
  protected val ORACLE = Keyword("ORACLE")
  protected val PUSHDOWN = Keyword("PUSHDOWN")

  def parseExtensions(input: String): ParseResult[LogicalPlan] = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input))
  }

  override protected def start: Parser[LogicalPlan] =
    sparkOraVersion | explainPushdown


  private lazy val sparkOraVersion : Parser[LogicalPlan] =
    SPARK_ORA ~ VERSION ^^ {
      case _ => SparkOraVersion()
    }

  private lazy val explainPushdown : Parser[LogicalPlan] =
    (EXPLAIN ~ ORACLE ~ PUSHDOWN) ~> restInput ^^ {
      case sqlText =>
        val df = sparkSession.sql(sqlText)
        ExplainPushdown(df.queryExecution.sparkPlan)
    }
}