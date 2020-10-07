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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.connector.read.{
  InputPartition,
  PartitionReader,
  PartitionReaderFactory
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.oracle.operators.OraPlan

case class OraPartition(
    index: Int,
    dsInfo: DataSourceInfo,
    outputSchema: AttributeSet,
    oraSQL: String,
    bindValues: Array[Any],
    override val preferredLocations: Array[String])
    extends Partition
    with InputPartition

object OraPartition {

  def apply(
      dsKey: DataSourceKey,
      index: Int,
      oraPlan: OraPlan,
      preferredLocations: Array[String]): OraPartition = {
    val (oraQry, bindValues) = OraPlan.generateOraSQL(oraPlan)
    val dsInfo = ConnectionManagement.info(dsKey)
    new OraPartition(
      index,
      dsInfo,
      oraPlan.catalystOutputSchema,
      oraQry,
      bindValues,
      preferredLocations)
  }

}

case class OraPartitionReaderFactory(sqlConf: SQLConf) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    assert(partition.isInstanceOf[OraPartition])
    val oraPartition = partition.asInstanceOf[OraPartition]

    // TODO
    ???
  }
}
