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

import oracle.spark.{ConnectionManagement, DataSourceKey, ORAMetadataSQLs}
import org.fusesource.leveldbjni.JniDBFactory
import org.iq80.leveldb.{DB, Options}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraIdentifier
import org.apache.spark.util.{ShutdownHookManager, Utils}

private[oracle] class OracleMetadataManager(cMap: CaseInsensitiveMap[String]) extends Logging {

  val connInfo = OracleCatalogOptions.connectionInfo(cMap)
  val catalogOptions = OracleCatalogOptions.catalogOptions(cMap)

  val dsKey: DataSourceKey = if (!catalogOptions.use_metadata_cache) {
    val dsKey = ConnectionManagement.registerDataSource(connInfo)
    ORAMetadataSQLs.validateConnection(dsKey)
    dsKey
  } else {
    DataSourceKey("FAKE", "FAKE")
  }

  private val cacheLoc: File =
    catalogOptions.metadataCacheLoc.map(new File(_)).getOrElse(Utils.createTempDir())

  private val cache: DB = {
    val options = new Options
    options.createIfMissing(true)
    val db = JniDBFactory.factory.open(cacheLoc, options)

    log.info(s"Opened metadata_cache at ${cacheLoc.getAbsolutePath}")

    ShutdownHookManager.addShutdownHook { () =>
      log.info(s"closing metadata_cache at ${cacheLoc.getAbsolutePath}")
      db.close()
    }
    db
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
          ns -> tbls.map(OraIdentifier(ns, _): Identifier)
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
