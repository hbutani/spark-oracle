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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.read.oracle.{OraPushdownScan, OraScan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.operators.OraQueryBlock

// scalastyle:off
/**
 * '''Table of Pushdown Constraints:'''
 * - Constraints on applying a relational operation in an existing [[OraQueryBlock]]
 * - yes means that the operation can be part of the same [[OraQueryBlock]]
 * - no means that a new [[OraQueryBlock]] is setup on which the operation is
 * applied. Existing Query Block becomes the source of this [[OraQueryBlock]]
 * {{{
 *QBlk has/  Column  Proj  Filt  Join  O-Join  S-Join  A-Join  L-Join  Agg
 *Apply      Pruned
 *--------------------------------------------------------------------------
 *Col. Prun  yes     yes   yes   yes   yes     yes     yes     yes     yes
 *Proj       yes     yes   yes   yes   yes     yes     yes     yes     yes
 *Filt       yes     yes   yes   yes   no      yes     no      yes     no
 *Join       yes     no    yes   yes   yes     no      no      no      no
 *Out-Join   yes     no    yes   yes   yes     yes     yes     yes     no
 *Semi-Join  yes     no    yes   yes   no      yes     yes     yes     no
 *Anti-Join
 *Lat-Join
 *Agg
 * }}}
 *
 * - Application of Project requires that AttrRefs in projections be substituted based
 * on current projections
 * - Application of Filter requires that AttrRefs in projections be substituted based
 * on current projections
 */
trait OraPushdown {
  // scalastyle:on
  val inDSScan: DataSourceV2ScanRelation
  val inOraScan: OraScan
  val inQBlk: OraQueryBlock
  val pushdownCatalystOp: LogicalPlan
  val sparkSession: SparkSession

  lazy val currQBlk = if (inQBlk.canApply(pushdownCatalystOp)) {
    inQBlk
  } else {
    inQBlk.newBlockOnCurrent
  }

  private[rules] def pushdownSQL: Option[OraQueryBlock]

  def pushdown: Option[DataSourceV2ScanRelation] = {
    pushdownSQL.map { oraPlan =>
      val newOraScan = OraPushdownScan(sparkSession, inOraScan.dsKey, oraPlan)
      inDSScan.copy(
        scan = newOraScan,
        output = pushdownCatalystOp.output.asInstanceOf[Seq[AttributeReference]]
      )
    }
  }

}

case class ExpandPushDown(inDSScan: DataSourceV2ScanRelation,
                          inOraScan: OraScan,
                          inQBlk: OraQueryBlock,
                          pushdownCatalystOp: Expand,
                          sparkSession: SparkSession) extends OraPushdown {

  private[rules] def pushdownSQL: Option[OraQueryBlock] = None
}

case class AggregatePushDown(inDSScan: DataSourceV2ScanRelation,
                             inOraScan: OraScan,
                             inQBlk: OraQueryBlock,
                             pushdownCatalystOp: Aggregate,
                             sparkSession: SparkSession) extends OraPushdown {

  private[rules] def pushdownSQL: Option[OraQueryBlock] = None
}

