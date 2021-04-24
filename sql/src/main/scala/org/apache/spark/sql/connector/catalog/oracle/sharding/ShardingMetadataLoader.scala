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

import scala.collection.mutable.ArrayBuffer

import oracle.spark.{ConnectionManagement, DataSourceKey}
import oracle.spark.sharding.ORAShardSQLs

import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadataManager


class ShardingMetadataLoader(mdMgr : OracleMetadataManager) {


  val coordDSKey = mdMgr.dsKey
  val coordDSInfo = ConnectionManagement.info(coordDSKey)

  /*
   * PUBLIC API
   */
  def loadMetadata : ShardingMetadata = {
    // TODO
    null
  }

  def loadTableRouting(tbl : QualifiedTableName) : Unit = {

  }


  def loadInstances : Array[ShardInstance] = {

    ORAShardSQLs.listShardInstances(coordDSKey) {rs =>
      val buf = ArrayBuffer[ShardInstance]()
      while (rs.next()) {
        val name : String = rs.getString(1)
        val connectString : String = rs.getString(2)
        val shardURL = ShardingMetadata.shardURL(connectString)
        buf += ShardInstance(name, connectString, coordDSInfo.convertToShard(shardURL))
      }
      buf.toArray
    }
  }

  def loadReplicatedTableList : Set[QualifiedTableName] = {
    ORAShardSQLs.listReplicatedTables(coordDSKey) {rs =>
      val buf = ArrayBuffer[QualifiedTableName]()
      while (rs.next()) {
        val owner : String = rs.getString(1)
        val tName : String = rs.getString(2)
        buf += QualifiedTableName(owner, tName)
      }
      buf.toSet
    }
  }

}