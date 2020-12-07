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
import java.util.Locale

import oracle.spark.{ConnectionInfo, ConnectionManagement, DataSourceKey, ORAMetadataSQLs}
import org.fusesource.leveldbjni.JniDBFactory
import org.iq80.leveldb.{DB, Options}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.{OraIdentifier, OraTable}
import org.apache.spark.sql.oracle.OracleCatalogOptions
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Provides Oracle Metadata. Clients can ask for ''namespaces'',
 * ''tables'' and table details as ''OraTable''.
 *
 * It can operate in ''cache_only'' mode where it serves request from information in the
 * local cache. This should only be used for testing.
 *
 * Caching behavior:
 * - on startup namespace list and table lists are loaded from local disk cache or DB
 *   and cached in memory.
 * - table metadata is loaded into local disk cache on demand.
 * - steady state is served from in-memory namespace & table lists + on-disk cached table metadata
 * - 'invalidateTable' deletes on-disk cached table metadata.
 * - 'reloadCatalog' reload in-memory and on-disk cache of namespace list and table lists
 *   - This doesn't clear individual table metadata
 *
 * @param cMap
 */
private[oracle] class OracleMetadataManager(cMap: CaseInsensitiveMap[String]) extends Logging {

  val connInfo = ConnectionInfo.connectionInfo(cMap)
  val catalogOptions = OracleCatalogOptions.catalogOptions(cMap)

  val cache_only = catalogOptions.metadata_cache_only

  val dsKey: DataSourceKey = {

    val setupPool = !catalogOptions.metadata_cache_only ||
      !catalogOptions.use_resultset_cache

    val dsKey = ConnectionManagement.registerDataSource(connInfo, catalogOptions, setupPool)

    if (setupPool) {
      ORAMetadataSQLs.validateConnection(dsKey)
    }

    dsKey
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

  @volatile private var _namespacesMap: CaseInsensitiveMap[String] = null
  @volatile private var _namespaces : Array[Array[String]] = null
  @volatile private var _tableMap: CaseInsensitiveMap[Set[String]] = null

  private[oracle] val defaultNamespace: String = dsKey.userName

  /*
   * On start load namespaces and table lists from local cache or DB.
   */
  loadNamespaces(!cache_only)
  loadTableSpaces(!cache_only)

  private def loadNamespaces(reload : Boolean = false) : Unit = {

    val nsBytes : Array[Byte] = if (!reload) {
      cache.get(OracleMetadata.NAMESPACES_CACHE_KEY)
    } else null

    val nsSet = if (nsBytes != null) {
      Serialization.deserialize[Set[String]](nsBytes)
    } else {
      val accessibleUsers = ORAMetadataSQLs.listAccessibleUsers(dsKey).toSet

      cache.put(
        OracleMetadata.NAMESPACES_CACHE_KEY,
        Serialization.serialize[Set[String]](accessibleUsers))
      accessibleUsers
    }
    _namespacesMap = CaseInsensitiveMap(nsSet.map(n => n -> n).toMap)
    _namespaces = _namespacesMap.values.map(ns => Array(ns)).toArray
  }

  private def loadTableSpaces(reload : Boolean = false) : Unit = {

    val tListBytes : Array[Byte] = if (!reload) {
      cache.get(OracleMetadata.TABLE_LIST_CACHE_KEY)
    } else null

    val tablMap: Map[String, Array[String]] = if (tListBytes != null) {
      Serialization.deserialize[Map[String, Array[String]]](tListBytes)
    } else {
      val tblMap = ORAMetadataSQLs.listAllTables(dsKey)

      cache.put(
        OracleMetadata.TABLE_LIST_CACHE_KEY,
        Serialization.serialize[Map[String, Array[String]]](tblMap))
      tblMap
    }
    _tableMap = CaseInsensitiveMap(tablMap.mapValues(_.toSet))
  }


  private[oracle] def namespaces : Array[Array[String]] = {
    _namespaces
  }

  private[oracle] def namespaceExists(ns: String): Boolean = {
    _namespacesMap.contains(ns)
  }

  private[oracle] def tableMap : CaseInsensitiveMap[Set[String]] = {
    _tableMap
  }

  private def tableKey(schema: String, table: String): (String, String, Array[Byte]) = {
    val oraSchema = _namespacesMap(schema)
    val oraTblNm = if (_tableMap(oraSchema).contains(table)) {
      table
    } else table.toUpperCase(Locale.ROOT)

    val tblId = OraIdentifier(Array(oraSchema), oraTblNm)
    val tblIdKey = Serialization.serialize(tblId)
    (oraSchema, oraTblNm, tblIdKey)
  }

  private[oracle] def oraTable(schema: String, table: String): OraTable = {
    val (oraSchema, oraTblNm, tblIdKey) = tableKey(schema, table)
    val tblMetadataBytes = cache.get(tblIdKey)

    if (tblMetadataBytes != null) {
      Serialization.deserialize[OraTable](tblMetadataBytes)
    } else {
      val oraTbl = oraTableFromDB(oraSchema, oraTblNm)
      val tblMetadatBytes = Serialization.serialize(oraTbl)
      cache.put(tblIdKey, tblMetadatBytes)
      oraTbl
    }
  }

  private def oraTableFromDB(schema: String, table: String): OraTable = {
    if (!cache_only) {
    val (xml, sxml) = ORAMetadataSQLs.tableMetadata(dsKey, schema, table)
    XMLReader.parseTable(xml, sxml)
    } else {
      OracleMetadata.unsupportedAction(
        "retrieve db info in 'cache_ony' mode",
        Some("set 'spark.sql.catalog.oracle.use_metadata_cache' to false"))
    }
  }

  private[oracle] def invalidateTable(schema: String, table: String): Unit = {
    if (!cache_only) {
      val (_, _, tblIdKey) = tableKey(schema, table)
      cache.delete(tblIdKey)
    }
  }

  private[oracle] def reloadCatalog : Unit = {
    loadNamespaces(true)
    loadTableSpaces(true)
  }

}
