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

package org.apache.spark.sql.connector.read.oracle

import java.util.{Locale, OptionalLong}

import oracle.hcat.db.split.OracleDBSplit
import oracle.spark.DataSourceKey
import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReaderFactory,
  Scan,
  Statistics,
  SupportsReportPartitioning,
  SupportsReportStatistics
}
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.execution.datasources.{
  FilePartition,
  InMemoryFileIndex,
  PartitioningAwareFileIndex
}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.oracle.operators.OraPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait OraScan {
  // scalastyle:off line.size.limit
  self: Scan
    with Batch
    with SupportsReportStatistics
    with SupportsMetadata
    with SupportsReportPartitioning =>
  // scalastyle:on

  def sparkSession: SparkSession
  def dsKey: DataSourceKey
  def oraPlan: OraPlan

  @transient protected lazy val (dbSplits: Array[OracleDBSplit], partitioning: Partitioning) =
    OraQuerySplitting.generateSplits(dsKey, oraPlan)

  override def planInputPartitions(): Array[InputPartition] = {
    /*
     - need to return Array of OraPartition
     - start with OraPlan + dsKey
     - ask OraQuerySplitting to generate Array[OracleDBSplit]
     - for each split
         ask OraQuerySplitting to applySplit to OraPlan -> to get OraPlan for split
         call OraPlan.generateOraSQL to generate sql + bindValues for OraPlan for split
         call OraQuerySplitting.preferedLocations to get preferred Locs for split
         call OraPartition.apply  passing idx, dsKey, oraSQL,  bindVals
     */

    for ((dbSplit, i) <- dbSplits.zipWithIndex) yield {
      val splitOraPlan = OraQuerySplitting.applySplit(oraPlan, dbSplit)
      val prefLocs = OraQuerySplitting.preferedLocations(dbSplit)
      OraPartition(dsKey, i, splitOraPlan, prefLocs)
    }
  }

  override def outputPartitioning(): Partitioning = partitioning

  override def createReaderFactory(): PartitionReaderFactory = {
    OraPartitionReaderFactory(sparkSession, oraPlan.catalystOutput)
  }

  override def estimateStatistics(): Statistics = {
    val oTbl = OraPlan.useTableStatsForPlan(oraPlan)
    oTbl
      .map { t =>
        val tStats = t.tabStats
        val nRows = tStats.row_count
        val bySz: Option[Long] = for (r <- nRows;
                                      s <- tStats.avg_row_size_bytes) yield (r * s).toLong

        new Statistics {
          override def sizeInBytes(): OptionalLong =
            bySz.map(OptionalLong.of).getOrElse(OptionalLong.empty())
          override def numRows(): OptionalLong =
            nRows.map(OptionalLong.of).getOrElse(OptionalLong.empty())
        }
      }
      .getOrElse(OraScan.UNKNOWN_ORA_STATS)
  }

}

case class OraFileScan(
    sparkSession: SparkSession,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    dsKey: DataSourceKey,
    oraPlan: OraPlan,
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression])
    extends FileScan
    with SupportsReportPartitioning
    with OraScan {

  lazy val fileIndex: PartitioningAwareFileIndex = {
    new InMemoryFileIndex(
      sparkSession,
      Seq.empty,
      options.asCaseSensitiveMap.asScala.toMap,
      Some(dataSchema))
  }

  /**
   * Why the tracking of pushed `partitionFilters` and `dataFilters`?
   * [[PruneFileSourcePartitions]] converts a
   * [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation]]
   * by calling this method and then since it does a `transformDown` rewrite
   * it turns around and calls the rewrite on the new Plan. This
   * plan has a [[org.apache.spark.sql.catalyst.plans.logical.Filter]]
   * on top of a new `DataSourceV2ScanRelation` with the original
   * predicates, which causes a recrusive invocation on the same filters
   * on top of a `DataSourceV2ScanRelation`, this keeps going for ever...
   * causing a StackOverflowError.
   *
   * @param pFilters
   * @param dFilters
   * @return
   */
  override def withFilters(pFilters: Seq[Expression], dFilters: Seq[Expression]): FileScan = {
    val newPFilters = pFilters != partitionFilters
    val newDFilters = dFilters != dataFilters
    if (newPFilters || newDFilters) {
      val oraPlanWithFilters = OraPlan.filter(oraPlan, pFilters, dFilters)
      this.copy(oraPlan = oraPlanWithFilters, partitionFilters = pFilters, dataFilters = dFilters)
    } else {
      this
    }
  }

  override def hashCode(): Int = oraPlan.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case o: OraFileScan =>
      readSchema == o.readSchema && oraPlan == o.oraPlan
    case _ => false
  }

  override def description(): String = super.description()

  override def getMetaData(): Map[String, String] = {
    Map(
      "Format" -> s"${this.getClass.getSimpleName.replace("Scan", "").toLowerCase(Locale.ROOT)}",
      "ReadSchema" -> readDataSchema.catalogString,
      "PartitionSchema" -> readPartitionSchema.catalogString,
      "dsKey" -> dsKey.toString,
      "OraPlan" -> oraPlan.numberedTreeString)
  }

  override protected def partitions: Seq[FilePartition] = {
    // defend against this being called
    throw new IllegalAccessException(
      "request to build file partitions shouldn't be called in an OraScan object")
  }

}

case class OraPushdownScan(sparkSession: SparkSession, dsKey: DataSourceKey, oraPlan: OraPlan)
    extends Scan
    with Batch
    with SupportsReportStatistics
    with SupportsMetadata
    with SupportsReportPartitioning
    with OraScan {

  lazy val readSchema = StructType.fromAttributes(oraPlan.catalystOutputSchema.toSeq)

  override def hashCode(): Int = oraPlan.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case o: OraPushdownScan => oraPlan == o.oraPlan
    case _ => false
  }

  override def description(): String = super.description()

  override def getMetaData(): Map[String, String] = {
    Map("dsKey" -> dsKey.toString, "OraPlan" -> oraPlan.numberedTreeString)
  }
}

object OraScan {
  private val UNKNOWN_ORA_STATS = new Statistics {
    override def sizeInBytes(): OptionalLong = OptionalLong.empty()
    override def numRows(): OptionalLong = OptionalLong.empty()
  }
}
