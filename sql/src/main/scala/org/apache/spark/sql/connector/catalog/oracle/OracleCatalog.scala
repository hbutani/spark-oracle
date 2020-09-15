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

import scala.jdk.CollectionConverters.mapAsJavaMapConverter

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.oracle.OracleMetadata.OraIdentifier
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * [[CatalogPlugin]] for an Oracle instance.
 * Provides [[TableCatalog]], [[SupportsNamespaces]], [[StagingTableCatalog]] and
 * [[CatalogExtension]] functionality.
 * Configure by setting
 * `spark.sql.catalog.oracle=org.apache.spark.sql.connector.catalog.oracle.OracleCatalog`
 *
 * Must provide connection information such as:
 * `spark.sql.catalog.oracle.url,spark.sql.catalog.oracle.user,`
 * `spark.sql.catalog.oracle.password,spark.sql.catalog.oracle.sun.security.krb5.principal,`
 * `spark.sql.catalog.oracle.kerbCallback,spark.sql.catalog.oracle.java.security.krb5.conf,`
 * `spark.sql.catalog.oracle.net.tns_admin,spark.sql.catalog.oracle.authMethod`
 */
class OracleCatalog extends CatalogPlugin with CatalogExtension with StagingTableCatalog {

  private var _name: String = _
  private var metadataManager: OracleMetadataManager = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    import scala.collection.JavaConverters._
    val cMap: CaseInsensitiveMap[String] = CaseInsensitiveMap(options.asScala.toMap)
    metadataManager = new OracleMetadataManager(cMap)
    _name = name
  }

  override def name(): String = _name

  override def defaultNamespace: Array[String] = Array(metadataManager.defaultNamespace)

  override def setDelegateCatalog(delegate: CatalogPlugin): Unit = ???

  override def listNamespaces(): Array[Array[String]] = metadataManager.namespaces

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    if (namespace.isEmpty) {
      listNamespaces()
    } else {
      Array.empty
    }
  }

  override def namespaceExists(namespace: Array[String]): Boolean = {
    if (namespace.size == 1) {
      metadataManager.namespaceExists(namespace.head)
    } else {
      super.namespaceExists(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    Map(
      SupportsNamespaces.PROP_OWNER -> metadataManager.dsKey.userName,
      SupportsNamespaces.PROP_LOCATION -> metadataManager.dsKey.connectionURL).asJava
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit = {
    OracleMetadata.unsupportedAction(s"create namespace", Some("create schema using Oracle DDL"))
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    OracleMetadata.unsupportedAction(
      s"alter namespace: " +
        s"${changes.map(_.getClass.getSimpleName).mkString("[", ", ", "]")}")
  }

  override def dropNamespace(namespace: Array[String]): Boolean = {
    OracleMetadata.unsupportedAction(s"drop namespace", Some("drop schema using Oracle DDL"))
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    metadataManager.tableMap
      .get(namespace.head)
      .map {
        case tSet => tSet.map(OraIdentifier(namespace, _): Identifier).toArray
      }
      .getOrElse(Array.empty)
  }

  override def loadTable(ident: Identifier): Table = ???

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = ???

  override def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  override def dropTable(ident: Identifier): Boolean = ???

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???

  override def stageCreate(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = ???

  override def stageReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = ???

  override def stageCreateOrReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable = ???

  private[oracle] def getMetadataManager: OracleMetadataManager = metadataManager
}
