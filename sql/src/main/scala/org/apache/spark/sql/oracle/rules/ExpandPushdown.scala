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
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.oracle.operators.{OraLateralJoin, OraQueryBlock}

case class ExpandPushdown(
    inDSScan: DataSourceV2ScanRelation,
    inOraScan: OraScan,
    inQBlk: OraQueryBlock,
    pushdownCatalystOp: Expand,
    sparkSession: SparkSession)
    extends OraPushdown
    with PredicateHelper {

  private[rules] def pushdownSQL: Option[OraQueryBlock] = {
    val expand = pushdownCatalystOp
    if (currQBlk.canApply(pushdownCatalystOp)) {
      expand match {
        case OraLateralJoin(olJ, oraOut) =>
          Some(
            currQBlk.copyBlock(
              select = oraOut,
              latJoin = Some(olJ),
              catalystOp = Some(expand),
              catalystProjectList = expand.output))
        case _ => None
      }
    } else None
  }
}
