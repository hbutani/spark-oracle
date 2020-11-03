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
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.oracle.OraSparkUtils
import org.apache.spark.sql.oracle.rules.OraSQLPushdownRule
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

    if (!extraRules.contains(OraSQLPushdownRule)) {
      ss.sessionState.experimentalMethods.extraOptimizations =
        ss.sessionState.experimentalMethods.extraOptimizations :+ OraSQLPushdownRule
    }
  }

  override def parsePlan(sqlText: String): LogicalPlan = {
    ensurePushdownRuleRegistered(OraSparkUtils.currentSparkSession)
    baseParser.parsePlan(sqlText)
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

  override def parseRawDataType(sqlText: String): DataType =
  baseParser.parseDataType(sqlText)
}
