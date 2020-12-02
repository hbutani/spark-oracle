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

import oracle.spark.{ConnectionManagement, DataSourceInfo, DataSourceKey}

import org.apache.spark.Partition
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.oracle.OraPartition.OraQueryAccumulators
import org.apache.spark.sql.oracle.SQLSnippet
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}

case class OraPartition(
    index: Int,
    dsInfo: DataSourceInfo,
    oraPartSQL: String,
    oraPartSQLParams: Seq[Literal],
    override val preferredLocations: Array[String])
    extends Partition
    with InputPartition

object OraPartition {

  def apply(
             dsKey: DataSourceKey,
             index: Int,
             orasql: SQLSnippet,
             preferredLocations: Array[String]): OraPartition = {
    val dsInfo = ConnectionManagement.info(dsKey)
    new OraPartition(index, dsInfo, orasql.sql, orasql.params, preferredLocations)
  }

  case class OraQueryAccumulators(rowsRead: LongAccumulator, timeToFirstRow: DoubleAccumulator)

  private val ROW_READ_ACCUM_NAME = "oracle.query.rows_read"
  private val TIME_TO_FIRST_ROW_ACCUM_NAME = "oracle.query.time_to_first_row"

  def createAccumulators(sparkSession: SparkSession): OraQueryAccumulators = {
    val rr = sparkSession.sparkContext.longAccumulator(ROW_READ_ACCUM_NAME)
    val ttfr = sparkSession.sparkContext.doubleAccumulator(TIME_TO_FIRST_ROW_ACCUM_NAME)
    OraQueryAccumulators(rr, ttfr)
  }

}

case class OraPartitionReaderFactory(
    catalystOutput: Seq[Attribute],
    accumulators: OraQueryAccumulators)
    extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    assert(partition.isInstanceOf[OraPartition])
    val oraPartition = partition.asInstanceOf[OraPartition]

    OraPartitionReader(oraPartition, catalystOutput, accumulators)

  }
}
