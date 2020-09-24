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
  InputPartition,
  PartitionReaderFactory,
  Statistics,
  SupportsReportPartitioning
}
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.execution.datasources.{
  FilePartition,
  InMemoryFileIndex,
  PartitioningAwareFileIndex
}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.oracle.operators.OraPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class OraScan(
    sparkSession: SparkSession,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    dsKey: DataSourceKey,
    oraPlan: OraPlan,
    options: CaseInsensitiveStringMap)
    extends FileScan
    with SupportsReportPartitioning {

  lazy val fileIndex: PartitioningAwareFileIndex = {
    new InMemoryFileIndex(
      sparkSession,
      Seq.empty,
      options.asCaseSensitiveMap.asScala.toMap,
      Some(dataSchema))
  }

  /*
   * partitionFilters + dataFilters
   * encapsulated in OraPlan
   */
  override def partitionFilters: Seq[Expression] = Seq.empty

  override def dataFilters: Seq[Expression] = Seq.empty

  override def withFilters(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): FileScan = {
    val oraPlanWithFilters = OraPlan.filter(oraPlan, partitionFilters, dataFilters)
    this.copy(oraPlan = oraPlanWithFilters)
  }

  @transient private lazy val (dbSplits: Array[OracleDBSplit], partitioning: Partitioning) =
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
      val (oraSQL, bindValues) = OraPlan.generateOraSQL(splitOraPlan)
      val prefLocs = OraQuerySplitting.preferedLocations(dbSplit)
      OraPartition(i, dsKey, oraSQL, bindValues, prefLocs)
    }
  }

  override def outputPartitioning(): Partitioning = partitioning

  override def createReaderFactory(): PartitionReaderFactory = {
    OraPartitionReaderFactory(sparkSession.sessionState.conf)
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

  override def hashCode(): Int = oraPlan.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case o: OraScan =>
      readSchema == o.readSchema && oraPlan == o.oraPlan
    case _ => false
  }

  override def description(): String = super.description()

  override def getMetaData(): Map[String, String] = {
    Map(
      "Format" -> s"${this.getClass.getSimpleName.replace("Scan", "").toLowerCase(Locale.ROOT)}",
      "ReadSchema" -> readDataSchema.catalogString,
      "dsKey" -> dsKey.toString,
      "OraPlan" -> oraPlan.numberedTreeString)
  }

  override protected def partitions: Seq[FilePartition] = {
    // defend against this being called
    throw new IllegalAccessException(
      "request to build file partitions shouldn't be called in an OraScan object")
  }

}

object OraScan {
  private val UNKNOWN_ORA_STATS = new Statistics {
    override def sizeInBytes(): OptionalLong = OptionalLong.empty()
    override def numRows(): OptionalLong = OptionalLong.empty()
  }
}
