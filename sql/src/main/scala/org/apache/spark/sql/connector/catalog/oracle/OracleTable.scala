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

package org.apache.spark.sql.connector.catalog.oracle

import java.util

import oracle.spark.DataSourceKey
import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraTable
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.read.oracle.OraScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.connector.write.oracle.{
  OraWriteBuilder,
  OraWriteSpec
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class OracleTable(
    dsKey: DataSourceKey,
    oraTable: OraTable,
    override val properties: util.Map[String, String])
    extends StagedTable
    with SupportsRead
    with SupportsWrite
    with SupportsDelete {

  lazy val tableId = Identifier.of(Array(oraTable.schema), oraTable.name)

  lazy val name = tableId.toString

  lazy val schema = oraTable.catalystSchema

  override def capabilities(): util.Set[TableCapability] =
    Set(BATCH_READ, BATCH_WRITE, TRUNCATE, OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC).asJava

  override lazy val partitioning: Array[Transform] =
    oraTable.partitionScheme.map(_.transforms).getOrElse(Array.empty)

  override def commitStagedChanges(): Unit = ???

  override def abortStagedChanges(): Unit = ???

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    OraScanBuilder(SparkSession.active, dsKey, tableId, oraTable, options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {

    // TODO
    // check info.schema() matches oraTable.catalystSchema
    // add casts if needed? or is this handled by Logical Optimizer.

    OraWriteBuilder(
      OraWriteSpec(
        dsKey,
        oraTable,
        info.schema(),
        info.queryId()))
  }

  /*
   * TODO
   *  this represents delete dml
   *  encapsulates delete handling in a OraDelete class
   */
  override def deleteWhere(filters: Array[Filter]): Unit = ???

}
