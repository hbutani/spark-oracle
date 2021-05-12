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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.expressions.Named.{OraColumnRef, OraNamedExpression}
import org.apache.spark.sql.oracle.expressions.OraExpression
import org.apache.spark.sql.oracle.expressions.Subquery.OraSubqueryExpression
import org.apache.spark.sql.oracle.operators._
import org.apache.spark.sql.oracle.rules.OraFixColumnNames.ORA_FIXED_NAMES_TAG

/**
 * Reverse the process of [[OraFixColumnNames]]
 */
object OraUnfixColumnNames {

  def unfix(plan : LogicalPlan) : LogicalPlan = {
    plan foreachUp {
      case dsv2@DataSourceV2ScanRelation(_, oScan : OraScan, _) =>
        unfix(oScan.oraPlan)
        oScan.oraPlan.unsetTagValue(ORA_FIXED_NAMES_TAG)
        dsv2
      case _ => ()
    }
    plan
  }

  private def unfix(oE : OraExpression) : Unit = oE foreachUp {
    case oc : OraColumnRef =>
      oc.clearOraFixedAlias
      oc.clearOraFixedNm
      oc
    case nE : OraNamedExpression =>
      nE.clearOraFixedAlias
      nE
    case sqE : OraSubqueryExpression =>
      unfix(sqE.oraPlan)
    case _ => ()
  }

  private def unfix(oraTabScan : OraTableScan) : Unit = {
    oraTabScan.projections.foreach(unfix)
    oraTabScan.filter.foreach(unfix)
    oraTabScan.partitionFilter.foreach(unfix)
  }

  private def unfix(jc : OraJoinClause) : Unit = {
    jc.clearJoinAlias
    unfix(jc.joinSrc)
    jc.onCondition.foreach(unfix)
  }

  private def unfix(ljc : OraLateralJoin) : Unit = {

    def _unfix(olje : OraLatJoinProjEntry) : Unit = {
      olje.clearOraFixedAlias
      unfix(olje.oraExpr)
    }

    for(
      p <- ljc.projections;
      pe <- p.projectList
        ) {
      _unfix(pe)
    }

  }

  private def unfix(oQBlk : OraSingleQueryBlock) : Unit = {
    oQBlk.joins.foreach(unfix)
    oQBlk.latJoin.foreach(unfix)
    oQBlk.select.foreach(unfix)
    oQBlk.where.foreach(unfix)
    for(
      gs <- oQBlk.groupBy;
      oE <- gs
    ) {
      unfix(oE)
    }
    for(
      os <- oQBlk.orderBy;
      oE <- os
    ) {
      unfix(oE)
    }
  }

  private def unfix(compQBlk : OraCompositeQueryBlock) : Unit = {
    compQBlk.children.foreach(unfix)
  }

  private def unfix(oPlan : OraPlan) : Unit = oPlan match {
    case oT : OraTableScan => unfix(oT)
    case oQBlk : OraSingleQueryBlock => unfix(oQBlk)
    case compQBlk : OraCompositeQueryBlock => unfix(compQBlk)
  }

}
