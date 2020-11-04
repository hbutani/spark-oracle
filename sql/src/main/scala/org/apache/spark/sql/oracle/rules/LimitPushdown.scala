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
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.sql.oracle.expressions.{AND, OraBinaryOpExpression}
import org.apache.spark.sql.oracle.expressions.Predicates.RownumLimit
import org.apache.spark.sql.oracle.operators.OraQueryBlock

case class LimitPushdown(
    inDSScan: DataSourceV2ScanRelation,
    inOraScan: OraScan,
    inQBlk: OraQueryBlock,
    pushdownCatalystOp: GlobalLimit,
    sparkSession: SparkSession)
    extends OraPushdown
    with PredicateHelper {

  override private[rules] def pushdownSQL = {
    val limitOp = pushdownCatalystOp
    val maxRows = limitOp.maxRows
    if (currQBlk.canApply(pushdownCatalystOp) && maxRows.isDefined) {
      val oraExpression = RownumLimit(SQLSnippet.LTE, maxRows.get)
      val newFil = currQBlk.where
        .map(f => OraBinaryOpExpression(AND, f.catalystExpr, f, oraExpression))
        .getOrElse(oraExpression)

      Some(
        currQBlk.copy(
          where = Some(newFil),
          catalystOp = Some(limitOp),
          catalystProjectList = limitOp.output)
      )
    } else None
  }
}
