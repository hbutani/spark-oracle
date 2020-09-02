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

import java.util.Locale

import oracle.spark.ConnectionInfo

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

case class OracleCatalogOptions(
    maxPartitions: Int,
    partitionerType: String,
    fetchSize: Int,
    isChunkSplitter: Boolean,
    chunkSQL: Option[String])

object OracleCatalogOptions {

  import ConnectionInfo._
  private[oracle] def connectionInfo(parameters: CaseInsensitiveMap[String]): ConnectionInfo = {
    require(parameters.isDefinedAt(ORACLE_URL), s"Option '$ORACLE_URL' is required.")
    require(parameters.isDefinedAt(ORACLE_JDBC_USER), s"Option '$ORACLE_URL' is required.")

    ConnectionInfo(
      parameters(ORACLE_URL),
      parameters(ORACLE_JDBC_USER),
      parameters.get(ORACLE_JDBC_PASSWORD),
      parameters.get(SUN_SECURITY_KRB5_PRINCIPAL),
      parameters.get(KERB_AUTH_CALLBACK),
      parameters.get(JAVA_SECURITY_KRB5_CONF),
      parameters.get(ORACLE_NET_TNS_ADMIN),
      parameters.get(ORACLE_JDBC_AUTH_METHOD))
  }

  private val catalogOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    catalogOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val ORACLE_JDBC_MAX_PARTITIONS = newOption("maxPartitions")
  val ORACLE_JDBC_PARTITIONER_TYPE = newOption("partitionerType")
  val ORACLE_FETCH_SIZE = newOption("fetchSize")
  val ORACLE_JDBC_IS_CHUNK_SPLITTER = newOption("isChunkSplitter")
  val ORACLE_CUSTOM_CHUNK_SQL = newOption("customPartitionSQL")
  val ORACLE_PARALLELISM = newOption("useOracleParallelism")

  val DEFAULT_MAX_SPLITS = 1

  private[oracle] def catalogOptions(
      parameters: CaseInsensitiveMap[String]): OracleCatalogOptions = {
    require(parameters.isDefinedAt(ORACLE_URL), s"Option '$ORACLE_URL' is required.")
    require(parameters.isDefinedAt(ORACLE_JDBC_USER), s"Option '$ORACLE_URL' is required.")

    OracleCatalogOptions(
      parameters.getOrElse(ORACLE_JDBC_MAX_PARTITIONS, "1").toInt,
      parameters.getOrElse(ORACLE_JDBC_PARTITIONER_TYPE, "SINGLE_SPLITTER"),
      parameters.getOrElse(ORACLE_FETCH_SIZE, "10").asInstanceOf[String].toInt,
      parameters.getOrElse(ORACLE_JDBC_IS_CHUNK_SPLITTER, "true").toBoolean,
      parameters.get(ORACLE_CUSTOM_CHUNK_SQL))
  }

}
