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

import java.io.File

import oracle.spark.{ConnectionManagement, ORAMetadataSQLs}
import org.fusesource.leveldbjni.JniDBFactory
import org.iq80.leveldb.{DB, Options}

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.util.Utils

private[oracle] class OracleMetadataManager(cMap: CaseInsensitiveMap[String]) {

  val connInfo = OracleCatalogOptions.connectionInfo(cMap)
  val catalogOptions = OracleCatalogOptions.catalogOptions(cMap)
  val dsKey = {
    val dsKey = ConnectionManagement.registerDataSource(connInfo)

    if (!catalogOptions.isTestEnv) {
      ORAMetadataSQLs.validateConnection(dsKey)
    }

    dsKey
  }

  private val cacheLoc: File = {
    if (catalogOptions.isTestEnv) {
      new File("src/test/resources/metadata_cache")
    } else {
      catalogOptions.metadataCacheLoc.map(new File(_)).getOrElse(Utils.createTempDir())
    }
  }

  private val cache: DB = {
    val options = new Options
    options.createIfMissing(true)
    JniDBFactory.factory.open(cacheLoc, options)
  }

  private[oracle] val namespaces: Array[Array[String]] = {
    val nsBytes = cache.get(OracleMetadata.NAMESPACES_CACHE_KEY)

    if (nsBytes != null) {
      Serialization.deserialize[Array[Array[String]]](nsBytes)
    } else {
      val accessibleUsers = ORAMetadataSQLs.listAccessibleUsers(dsKey).map(u => Array[String](u))

      cache.put(
        OracleMetadata.NAMESPACES_CACHE_KEY,
        Serialization.serialize[Array[Array[String]]](accessibleUsers))
      accessibleUsers
    }
  }

  private[oracle] val defaultNamespace: Array[String] = Array(dsKey.userName)

  private[oracle] val tableMap: Map[Array[String], Array[Identifier]] = {
    val tListBytes = cache.get(OracleMetadata.TABLE_LIST_CACHE_KEY)

    if (tListBytes != null) {
      Serialization.deserialize[Map[Array[String], Array[Identifier]]](tListBytes)
    } else {
      val tblMap = ORAMetadataSQLs.listAllTables(dsKey).map {
        case (u, tbls) =>
          val ns = Array(u)
          ns -> tbls.map(Identifier.of(ns, _))
      }

      cache.put(
        OracleMetadata.TABLE_LIST_CACHE_KEY,
        Serialization.serialize[Map[Array[String], Array[Identifier]]](tblMap))
      tblMap
    }
  }

  private[oracle] def oraTable(schema: String, table: String): OracleTable = {
    ???
  }

}
