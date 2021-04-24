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

package org.apache.spark.sql.connector.catalog.oracle.sharding

import oracle.spark.DataSourceInfo

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraColumn

case class ShardInstance(name : String, connectString : String, shardDSInfo : DataSourceInfo)

case class ChunkInfo(shardName: String,
                     shardKeyLo: Array[Byte],
                     shardKeyHi: Array[Byte],
                     groupKeyLo: Array[Byte],
                     groupKeyHi: Array[Byte],
                     chunkId: Int,
                     groupId: Int,
                     uniqueId: Int,
                     name: String,
                     priority: Int,
                     state: Int)

case class ShardTable(qName : QualifiedTableName,
                      tableFamilyId : Int,
                      superKeyColumns : Array[OraColumn],
                      keyColumns : Array[OraColumn])

case class TableFamily(id : Int,
                       superShardType : ShardingType,
                       shardType : ShardingType,
                       rootTable : ShardTable,
                       version : Int)


/*
 * attached to OracleCatalog through an Atomic reference
 * - loaded on Catalog setup & on issue of reload
 * - Routing information per tblFamily loaded on demand or explicit show command
 *
 * Supported Commands:
 * - list Shard Instances
 * - list Table Families
 * - list replicated Tables
 * - Reload Sharding Metadata
 * - Reload Routing for Table
 * - List Routing for Table
 * - List Executor-Shard Affinity
 *
 */
class ShardingMetadata {

}

object ShardingMetadata {

  trait ShardingException extends AnalysisException

  class UnsupportedShardingType(shardType: String,
                                reason: Option[String])
    extends AnalysisException(
      s"Unsupported ShardType ${shardType}" +
        s" ${if (reason.isDefined) "," + reason.get else ""}")

  def unsupportedShardType(typeNm: String,
                           reason: Option[String] = None): Nothing =
    throw new UnsupportedShardingType(typeNm, reason)

  def shardURL(shardConnectStr : String) : String = s"jdbc:oracle:thin:@//${shardConnectStr}"
}