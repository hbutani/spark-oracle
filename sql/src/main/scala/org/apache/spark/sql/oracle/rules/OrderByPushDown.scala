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
import org.apache.spark.sql.catalyst.expressions.{Expression, NullOrdering, SortDirection, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Sort}
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.sql.oracle.expressions.{OraExpression, OraExpressions, OraLiteralSql}
import org.apache.spark.sql.oracle.operators.OraQueryBlock

case class OrderByPushDown(inDSScan: DataSourceV2ScanRelation,
                           inOraScan: OraScan,
                           inQBlk: OraQueryBlock,
                           pushdownCatalystOp: Sort,
                           sparkSession: SparkSession
                          ) extends OraPushdown with ProjectListPushdownHelper {

  def sortByOrder(order: Seq[SortOrder],
                  global: Boolean,
                  child: LogicalPlan): Option[Seq[OraExpression]] = {
    // Go Through Each Sort Order and make a seq of OraExpression.
    val sq: Option[Seq[OraExpression]] = OraExpressions.unapplySeq(order)
    sq
  }

  override private[rules] def pushdownSQL: Option[OraQueryBlock] = {
    if (currQBlk.canApply(pushdownCatalystOp)) {
      val sortOpp = pushdownCatalystOp
      val sortExpression = pushdownCatalystOp.order
      val res = sortByOrder(pushdownCatalystOp.order,
        pushdownCatalystOp.global, pushdownCatalystOp.child)
      Some(currQBlk.copyBlock(orderBy = res,
        catalystOp = Some(sortOpp)))
    } else {
      None
    }
  }
}
