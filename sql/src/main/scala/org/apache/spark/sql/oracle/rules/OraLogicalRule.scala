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
package org.apache.spark.sql.oracle.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.oracle.OraSparkUtils

abstract class OraLogicalRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {

    import org.apache.spark.sql.oracle.OraSparkConfig._

    implicit val sparkSession = OraSparkUtils.currentSparkSession

    val rewrite: Boolean = getConf(ENABLE_ORA_PUSHDOWN)(sparkSession)

    if (rewrite) {
      _apply(plan)
    } else plan
  }

  def _apply(plan: LogicalPlan)(implicit sparkSession : SparkSession): LogicalPlan

}

object OraLogicalRules extends OraLogicalRule {

  val RULES = Seq(OraSQLPushdownRule, OraFixColumnNames)

  override def _apply(plan: LogicalPlan)(implicit sparkSession: SparkSession): LogicalPlan = {
    RULES.foldLeft(plan) {
      case (plan, r) => r(plan)
    }
  }
}

/*
 * Translation Notes:
 * Plan Matching Pattern is
 *  - DataSourceV2ScanRelation(OracleTable, OraScan, output: Seq[AttributeReference])
 *  - OraScan can be OraFileScan or OraPushdownScan
 *
 * OraScan has a oraPlan: OraPlan
 *
 * Walk through 2-3 tpcds query translations.
 */
