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

import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.connector.read.oracle.OraScan
import org.apache.spark.sql.oracle.operators.OraTableScan

/**
 * '''Query Splitting''' is used to split a oracle pushdown query into a set of
 * queries such that the union-all of the results is the same as the original query result.
 *
 * Each split query is invoked in a seperate partition of the [[OraScan]].
 * Query splitting is trigerred when Spark requests the [[OraScan]] instance for
 * [[OraScan.planInputPartitions planInputPartitions]] or
 * [[OraScan.outputPartitioning outputPartitioning]].
 *
 * See further information on [[OraDBSplit schemes for splitting]],
 * [[QuerySplitAnalyzer what schemes apply to different classes of Queries]],
 * [[OraExplainPlan plan stats gathered]], and how [[OraSplitStrategy split analysis is done]].
 */
package object querysplit {

  case class TableAccessOperation(tabNm : String,
                                  qBlk : String,
                                  object_alias : String,
                                  alias : String,
                                  row_count : Long,
                                  bytes : Long,
                                  partitionRange : Option[(Int, Int)])

  case class PlanInfo(
                       rowCount : Long,
                       bytes : Long,
                       tabAccesses : Seq[TableAccessOperation]
                     )

  def ORA_SYS_PARTITION(pNm : String) : Boolean = pNm.startsWith("SYS")

  case class TableAccessDetails(oraTable : OraTable, tabAccessOp : TableAccessOperation) {
    val scheme = oraTable.schema
    val name = oraTable.name
    val tableParts : Option[Seq[String]] = tabAccessOp.partitionRange.flatMap { r =>
      val partNms = oraTable.partitions(r._1, r._2).filterNot(ORA_SYS_PARTITION)
      if (partNms.isEmpty) None else Some(partNms)
    }
  }

  case class SplitCandidate(alias : Option[String],
                            oraTabScan : OraTableScan)

  case class SplitScope(candidateTables : Seq[SplitCandidate])


}
