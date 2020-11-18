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
import org.apache.spark.sql.catalyst.expressions.{And, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.expressions.{AND, OraBinaryOpExpression, OraExpression}
import org.apache.spark.sql.oracle.operators.OraQueryBlock

case class FilterPushdown(inDSScan: DataSourceV2ScanRelation,
                          inOraScan: OraScan,
                          inQBlk: OraQueryBlock,
                          pushdownCatalystOp: Filter,
                          sparkSession: SparkSession)
  extends OraPushdown with PredicateHelper {

  private[rules] def pushdownSQL: Option[OraQueryBlock] = {
    if (currQBlk.canApply(pushdownCatalystOp)) {
      val filOp = pushdownCatalystOp

      val pushdownCondition =
        replaceAlias(filOp.condition, getAliasMap(currQBlk.catalystProjectList))
      val pushdownFilters = {
        val splitConds = splitConjunctivePredicates(pushdownCondition)
        val currConds: Set[Expression] = currQBlk.where.map { oraCond =>
          oraCond.collect {
            case oE => oE.catalystExpr.canonicalized
          }
        }.getOrElse(Seq.empty).toSet
        splitConds.filter(e => !currConds.contains(e.canonicalized))
      }

      if (pushdownFilters.nonEmpty) {
        val pushCond = pushdownFilters.reduceLeft(And)
        for (oraExpression <- OraExpression.unapply(pushCond)) yield {
          val newFil = currQBlk.where.map(f =>
            OraBinaryOpExpression(AND, And(f.catalystExpr, pushdownCondition), f, oraExpression)
          ).getOrElse(oraExpression)

          currQBlk.copyBlock(
            where = Some(newFil),
            catalystOp = Some(filOp),
            catalystProjectList = filOp.output
          )
        }
      } else {
        Some(currQBlk)
      }
    } else None
  }
}
