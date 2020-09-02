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

package oracle.spark

import java.sql.Connection
import java.util.concurrent.{ConcurrentHashMap => CMap}

import scala.language.implicitConversions

import oracle.hcat.db.conn.OracleDBConnectionCacheUtil
import oracle.spark.ORASQLUtils._

import org.apache.spark.sql.catalyst.TableIdentifier

case class DataSourceKey(connectionURL: String, userName: String)

object DataSourceKey {
  implicit def dataSourceKey(connInfo: ConnectionInfo): DataSourceKey =
    DataSourceKey(connInfo.url, connInfo.username)
}

case class DataSourceInfo(key: DataSourceKey, connInfo: ConnectionInfo, isSharded: Boolean)

trait DataSources {

  private val dsMap = new CMap[DataSourceKey, DataSourceInfo]()

  private[oracle] def registerDataSource(dsKey: DataSourceKey, connInfo: ConnectionInfo): Unit = {
    val createDSI = new java.util.function.Function[DataSourceKey, DataSourceInfo] {
      override def apply(t: DataSourceKey): DataSourceInfo = {
        val isSharded = setupConnectionPool(dsKey, connInfo)
        DataSourceInfo(dsKey, connInfo, isSharded)
      }
    }
    val dsInfo = dsMap.computeIfAbsent(dsKey, createDSI)
    if (dsInfo.connInfo != connInfo) {
      throwAnalysisException(s"""
           |Currently we require all table definitions to a database to have the
           |same connection properties:
           |Properties Already registered:
           |${dsInfo.connInfo.dump}
           |Properties specified:
           |${connInfo.dump}
         """.stripMargin)
    }
  }

  def registerDataSource(connInfo: ConnectionInfo): DataSourceKey = {
    val dsKey: DataSourceKey = connInfo
    registerDataSource(dsKey, connInfo)
    dsKey
  }

  private[oracle] def info(dsKey: DataSourceKey): DataSourceInfo = {
    import scala.collection.JavaConverters._
    dsMap.asScala.getOrElse(
      dsKey,
      throwAnalysisException(s"Couldn't find details about DataSource ${dsKey}"))
  }

  private[oracle] def isSharded(dsKey: DataSourceKey): Boolean = {
    info(dsKey).isSharded
  }

  private[oracle] def getConnection(dsKey: DataSourceKey): Connection = {
    ConnectionManagement.getConnection(info(dsKey))
  }

  private[oracle] def tableIdentifier(name: String): TableIdentifier = {
    val arr = OracleDBConnectionCacheUtil.splitTableName(name)
    new TableIdentifier(arr(1), Option(arr(0)))
  }

  private def setupConnectionPool(dsKey: DataSourceKey, connInfo: ConnectionInfo): Boolean = {
    val b = withConnection[Boolean](
      dsKey,
      ConnectionManagement.getConnection(dsKey, connInfo),
      "setup connection pool") { conn =>
      OracleDBConnectionCacheUtil.isShardedDB(conn)
    }
    b
  }

}
