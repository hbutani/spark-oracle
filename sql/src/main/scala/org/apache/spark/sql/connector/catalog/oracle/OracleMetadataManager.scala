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

private[oracle] class OracleMetadataManager(cMap: CaseInsensitiveMap[String]) extends Logging {

  val connInfo = ConnectionInfo.connectionInfo(cMap)
  val catalogOptions = OracleCatalogOptions.catalogOptions(cMap)

  val (dsKey: DataSourceKey, cache_only: Boolean) = if (!catalogOptions.use_metadata_cache) {
    val dsKey = ConnectionManagement.registerDataSource(connInfo, catalogOptions)
    ORAMetadataSQLs.validateConnection(dsKey)
    (dsKey, false)
  } else {
    (connInfo: DataSourceKey, true)
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

  private val namespacesMap: CaseInsensitiveMap[String] = {
    val nsBytes = cache.get(OracleMetadata.NAMESPACES_CACHE_KEY)

    val nsSet = if (nsBytes != null) {
      Serialization.deserialize[Set[String]](nsBytes)
    } else {
      val accessibleUsers = ORAMetadataSQLs.listAccessibleUsers(dsKey).toSet

      cache.put(
        OracleMetadata.NAMESPACES_CACHE_KEY,
        Serialization.serialize[Set[String]](accessibleUsers))
      accessibleUsers
    }
    CaseInsensitiveMap(nsSet.map(n => n -> n).toMap)
  }

  private[oracle] lazy val namespaces =
    namespacesMap.values.map(ns => Array(ns)).toArray

  private[oracle] def namespaceExists(ns: String): Boolean = namespacesMap.contains(ns)

  private[oracle] val defaultNamespace: String = dsKey.userName

  private[oracle] val tableMap: CaseInsensitiveMap[Set[String]] = {
    val tListBytes = cache.get(OracleMetadata.TABLE_LIST_CACHE_KEY)

    val tablMap: Map[String, Array[String]] = if (tListBytes != null) {
      Serialization.deserialize[Map[String, Array[String]]](tListBytes)
    } else {
      val tblMap = ORAMetadataSQLs.listAllTables(dsKey)

      cache.put(
        OracleMetadata.TABLE_LIST_CACHE_KEY,
        Serialization.serialize[Map[String, Array[String]]](tblMap))
      tblMap
    }
    CaseInsensitiveMap(tablMap.mapValues(_.toSet))
  }

  private def tableKey(schema: String, table: String): (String, String, Array[Byte]) = {
    val oraSchema = namespacesMap(schema)
    val oraTblNm = if (tableMap(oraSchema).contains(table)) {
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

}
