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
package org.apache.spark.sql.oracle.querysplit

import oracle.spark.DataSourceKey

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.oracle.OraUnknownDistribution
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.oracle.{OraSparkConfig, SQLSnippet}
import org.apache.spark.sql.oracle.operators.{OraPlan, OraTableScan}

trait OraSplitStrategy {

  val splitList : IndexedSeq[OraDBSplit]

  def splitIds : Range = (0 until splitList.size)

  /**
   * Based on how data is distributed among the [[OraDBSplit]]s
   * we can associate a [[Partitioning]] to the [[OraScan]]
   * that this Strategy applie to.
   * @return
   */
  def partitioning: Partitioning = OraUnknownDistribution(0)

  /**
   * A [[OraDBSplit]] potentially implies a block or partition restriction.
   * In the future this information can be used to come up with `preferred locations`
   * for a split.
   * - some way to associate block/partitions to physical hosts
   * - some environment information that associates spark nodes with oracle nodes
   *
   * @param splitId
   * @return
   */
  def preferredLocs(splitId : Int): Array[String] = Array.empty

  /**
   * [[OraDBSplit]] implies an optional rowid or partition range
   * to be applied to an Oracle Table. In an [[OraPlan]] this is
   * applied by replacing a [[OraTableScan]] reference with a
   * query block that selects the necessary table columns with the
   * implied row/partition range in the where/partition clause.
   *
   * @param oraTblScan
   * @param splitId
   * @return
   */
  def splitOraSQL(oraTblScan : OraTableScan, splitId : Int) : Option[SQLSnippet]

}

case object NoSplitStrategy extends OraSplitStrategy {
  val splitList = IndexedSeq(OraNoSplit)

  override def splitOraSQL(oraTblScan: OraTableScan, splitId : Int): Option[SQLSnippet] =
    None
}

/**
 * Algorithm for Splitting a Pushdown Query.
 *
 * '''Step 0:'''
 *  - If [[OraSparkConfig#ENABLE_ORA_QUERY_SPLITTING]] is false return
 *    [[NoSplitStrategy]]
 *
 * '''Step 1''' : analyze the [[OraPlan]] for splitting
 *  - run it through the [[QuerySplitAnalyzer]]
 *  - obtain the [[SplitScope]]
 *
 * '''Step 2:''' get Plan statistics from [[OraExplainPlan]]
 *  - get overall query `rowCount, bytes` and [[TableAccessOperation]]s if there
 *    is scope for [[OraPartitionSplit]], [[OraRowIdSplit]].
 *
 * '''Step 3:''' decide on SplitStrategy
 *  - If `planComplex && !resultSplitEnabled` then decide on [[NoSplitStrategy]]
 *    - A plan is complex if it has no candidate Tables for Splitting.
 *    - Since [[OraResultSplit]] may lead to lotof extra work on the Oracle instancw
 *      users can turn it off with the setting `spark.sql.oracle.allow.splitresultset`
 *  - Otherwise try to identify a table to split on based on [[SplitCandidate]]s.
 *    - Choose the table with the largest amount of bytes read.
 *    - Chosen table must have 10 times more bytes read than any other
 *      [[TableAccessOperation]]
 *  - Have the [[OraDBSplitGenerator]] generate splits based on query stats and optionally
 *    a target split table.
 *
 */
object OraSplitStrategy extends Logging {

  private def apply(splitList : IndexedSeq[OraDBSplit]) : OraSplitStrategy = {
    null
  }

  def generateSplits(dsKey : DataSourceKey,
                     oraPlan : OraPlan)(
      implicit sparkSession : SparkSession
  ) : OraSplitStrategy = {
    val qrySplitEnabled = OraSparkConfig.getConf(OraSparkConfig.ENABLE_ORA_QUERY_SPLITTING)

    if (!qrySplitEnabled || true /* TODO, currently query splitting not enabled */ ) {
      NoSplitStrategy
    } else {
      val splitScope = QuerySplitAnalyzer.splitCandidates(oraPlan)
      val planComplex = splitScope.candidateTables.nonEmpty
      val planInfoO = OraExplainPlan.constructPlanInfo(dsKey, oraPlan, planComplex)

      if (!planInfoO.isDefined) {
        NoSplitStrategy
      } else {
        val planInfo = planInfoO.get
        val bytesPerTask: Long = OraSparkConfig.getConf(OraSparkConfig.BYTES_PER_SPLIT_TASK)
        val resultSplitEnabled = OraSparkConfig.getConf(OraSparkConfig.ALLOW_SPLITBY_RESULTSET)

        if (planInfo.bytes < bytesPerTask || (planComplex && !resultSplitEnabled)) {
          NoSplitStrategy
        } else {
          val targetTable: Option[TableAccessDetails] =
            buildTableAccess(planInfo.tabAccesses, splitScope.candidateTables)
          val splitList: IndexedSeq[OraDBSplit] = new OraDBSplitGenerator(dsKey,
            planInfo.bytes,
            planInfo.rowCount,
            bytesPerTask,
            targetTable).generateSplitList
          OraSplitStrategy(splitList)
        }
      }
    }
  }

  private def matchOraTable(tblAccess : TableAccessOperation,
                   splitTables : Seq[SplitCandidate]) : Option[OraTableScan] = {
    val chosenTable = splitTables.find {sp =>
      sp.oraTabScan.oraTable.name == tblAccess.tabNm &&
        (!sp.alias.isDefined || s"${sp.alias.get}@${tblAccess.qBlk}" == tblAccess.alias)
    }

    if (!chosenTable.isDefined) {
      logWarning(
        s"""Failed to match OraTable for tableAccess: ${tblAccess}
           |  spliTables = ${splitTables.mkString(",")}""".stripMargin
      )
    }

    chosenTable.map(_.oraTabScan)
  }

  private val LARGE_TABLE_BYTES_READ_MULTIPLIER = 10

  private def buildTableAccess(tabAccesses : Seq[TableAccessOperation],
                       splitTables : Seq[SplitCandidate]) : Option[TableAccessDetails] = {

    val tbAcOp = tabAccesses.sortBy(tA => tA.bytes).last
    val isLargeTabAcc = tabAccesses.forall(tA => tA == tbAcOp ||
      tA.bytes < LARGE_TABLE_BYTES_READ_MULTIPLIER * tbAcOp.bytes
    )
    for(tblScan <- matchOraTable(tbAcOp, splitTables) if isLargeTabAcc) yield {
      tblScan.setQuerySplitCandidate
      TableAccessDetails(tblScan.oraTable, tbAcOp)
    }
  }
}
