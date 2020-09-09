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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.oracle.OraSparkUtils

object OracleMetadata {

  trait OracleMetadataException extends AnalysisException

  class UnsupportedOraDataType(typeNm: String, reason: Option[String])
      extends AnalysisException(
        s"Unsupported Datatype ${typeNm}" +
          s" ${if (reason.isDefined) "," + reason.get else ""}")

  def unsupportedOraDataType(typeNm: String): Nothing =
    throw new UnsupportedOraDataType(typeNm, None)

  def checkDataType(cond: Boolean, typeNm: => String, reason: => String): Unit = {
    if (!cond) {
      throw new UnsupportedOraDataType(typeNm, Some(reason))
    }
  }

  object OraPartitionType extends Enumeration {
    val RANGE = Value("RANGE")
    val LIST = Value("LIST")
    val HASH = Value("HASH")

    def apply(s: String): Value = s match {
      case "RANGE_PARTITIONING" => RANGE
      case "RANGE_SUBPARTITIONING" => RANGE
      case "LIST_PARTITIONING" => LIST
      case "LIST_SUBPARTITIONING" => LIST
      case "HASH_PARTITIONING" => HASH
      case "HASH_SUBPARTITIONING" => HASH
      case _ => OraSparkUtils.throwAnalysisException(s"Unsupported Partition type '${s}'")
    }
  }

  case class OraColumn(
      name: String,
      dataType: OraDataType,
      notNull: Boolean,
      precision: Int,
      scale: Option[Int])

  case class OraPrimaryKey(cols: Array[String])

  case class OraForeignKey(
      cols: Array[String],
      referencedTable: (String, String),
      referencedCols: Array[String])

  case class OraTablePartition(
      name: String,
      idx: Int,
      values: String,
      subPartitions: Array[OraTablePartition])

  case class TablePartitionScheme(
      columns: Array[String],
      partType: OraPartitionType.Value,
      subPartitionScheme: Option[TablePartitionScheme])

  case class OraTable(
      schema: String,
      name: String,
      columns: Array[OraColumn],
      partitionScheme: Option[TablePartitionScheme],
      partitions: Array[OraTablePartition],
      primaryKey: Option[OraPrimaryKey],
      foreignKeys: Array[OraForeignKey],
      is_external: Boolean,
      num_blocks: Long,
      block_size: Int,
      row_count: Long,
      avg_row_size_bytes: Double,
      properties: util.Map[String, String])

  /*
   * backed by a LevelDB
   * key is (schema, tblNm); value is OracleTableMetadata
   *
   */

  // https://medium.com/@wishmithasmendis/leveldb-from-scratch-in-java-c300e21c7445

  /*
 * Todo
 *  - define metadata classes
 *  - xml reader from sxml to classes
 *  - kyro serializer
 *  - what do i need for dbsplits ?
 *  - levelDB infra
 *  - read method -> get from cache or read from db + put
 *
 */

}
