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

import oracle.hcat.db.conn.OracleDBConnectionCache
import oracle.ucp.jdbc.PoolDataSource
import org.apache.spark.internal.Logging

/**
 * '''Connection Managment''' for Driver and Executor jvms.
 * Consumers should only use the ''getConnection(dsInfo : DataSourceInfo)'' method.
 *
 * Internally maintains a [[PoolDataSource]] per [[DataSourceKey]]; also maintains a
 * [[Connection]] with the current thread. On a ''getConnection'' call attempts to serve
 * the request from the COnnection associated with the calling thread; if not it returns
 * currrent connection(if open) back to its pool before associating a Connection of the
 * requested DataSourceKey with the calling Thread and returning it.
 *
 * Pool is setup using ''getNewPDS'' function from [[OracleDBConnectionCache]] class
 * provided in ''oracle.hcat.db.conn'' code drop.
 * Pool set with `maxPoolSize = Runtime.availableProcessors()` and
 * `connectionWaitTime = 1 sec`.
 */
private[oracle] object ConnectionManagement extends DataSources with Logging {

  private val pdsMap = new CMap[DataSourceKey, PoolDataSource]()

  type THREAD_CONNECTION_TYPE = Option[(DataSourceKey, Connection)]

  private val _threadLocalConnection = new ThreadLocal[THREAD_CONNECTION_TYPE] {
    override def initialValue() = None
  }

  private def attachAndGetConnection(dsKey : DataSourceKey,
                                     pds : PoolDataSource) : Connection = {
    _threadLocalConnection.get() match {
      case Some((tDsKey, conn)) if (tDsKey == dsKey && !conn.isClosed) => conn
      case None => {
        logDebug(
          s"Connection request for ${dsKey} on ${Thread.currentThread().getName}"
        )
        val conn : Connection = pds.getConnection()
        _threadLocalConnection.set(Some(dsKey, conn))
        conn
      }
      case Some((tDsKey, conn)) => {
        logDebug(
          s"Connection request for ${dsKey} on ${Thread.currentThread().getName}," +
            s" when thread has open connection for ${tDsKey}"
        )
        if (!conn.isClosed) {
          conn.close()
        }
        _threadLocalConnection.set(None)
        attachAndGetConnection(dsKey, pds)
      }
    }
  }

  private def _setupPool(dsKey : DataSourceKey,
                         connInfo: ConnectionInfo) : PoolDataSource = {

    val createPDS = new java.util.function.Function[DataSourceKey, PoolDataSource] {
      override def apply(t: DataSourceKey): PoolDataSource = {
        val pds = OracleDBConnectionCache.getNewPDS(
          connInfo.authMethod.getOrElse(null),
          connInfo.asConnectionProperties,
          connInfo.url
        )
        logInfo(s"Setting up Connection Pool ${dsKey}")
        pds.setMaxPoolSize(Runtime.getRuntime.availableProcessors())
        pds.setConnectionWaitTimeout(1)

        pds
      }
    }
    pdsMap.computeIfAbsent(dsKey, createPDS)
  }

  private[oracle] def getConnection(dsKey : DataSourceKey,
                                    connInfo: ConnectionInfo) : Connection = {
    val pds = _setupPool(dsKey, connInfo)
    attachAndGetConnection(dsKey, pds)
  }

  def getConnection(dsInfo : DataSourceInfo) : Connection = {
    val pds = _setupPool(dsInfo.key, dsInfo.connInfo)
    attachAndGetConnection(dsInfo.key, pds)
  }

  def reset(): Unit = synchronized {
    import scala.collection.JavaConverters._
    for(pds <- pdsMap.values().asScala) {
      // TODO
    }
    pdsMap.clear()
  }

  def getConnectionInExecutor(ds : DataSourceInfo) : Connection = {
    ConnectionManagement.getConnection(ds)
  }

}
