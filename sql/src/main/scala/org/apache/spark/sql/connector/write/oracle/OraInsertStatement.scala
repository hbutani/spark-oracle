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

package org.apache.spark.sql.connector.write.oracle

import java.sql.{Connection, PreparedStatement, ResultSet}

import oracle.spark.{ConnectionManagement, DataSourceInfo}

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.connector.read.oracle.ConnectionCloser
import org.apache.spark.sql.oracle.OracleCatalogOptions
import org.apache.spark.sql.oracle.sqlexec.SparkOraStatement
import org.apache.spark.util.DoubleAccumulator


case class OraInsertStatement(datasourceInfo: DataSourceInfo,
                              sqlTemplate: String,
                              accumalators: OraDataWriter.OraInsertAccumulators
                             ) extends SparkOraStatement {

  val timeToExecute: DoubleAccumulator = accumalators.timeToWriteRows

  private var conn : Connection = null

  private val batchSize: Int = datasourceInfo.catalogOptions.fetchSize
  private var rowCount : Int = 0
  private var totalRowCount : Int = 0

  lazy val underlying: PreparedStatement = {
    conn = {
      val c = ConnectionManagement.getConnectionInExecutor(datasourceInfo)
      val tc = TaskContext.get()
      if (tc != null) {
        val l = ConnectionCloser(c)
        tc.addTaskCompletionListener(l)
        tc.addTaskFailureListener(l)
      } else {
        throw new IllegalStateException(
          "setting up a OraInsertStatement.preparedStatement on a thread with no TaskContext"
        )
      }
      c
    }
    val ps = conn.prepareStatement(sqlTemplate)
    ps
  }

  override def bindValues: Seq[Literal] = Seq.empty

  override def catalogOptions: OracleCatalogOptions = datasourceInfo.catalogOptions

  private def executeBatch : Unit = {
    val sTime = System.currentTimeMillis()
    underlying.executeBatch()
    val eTime = System.currentTimeMillis()
    timeToExecute.add(eTime - sTime)
    accumalators.rowsWritten.add(rowCount)
    rowCount = 0
  }

  def addBatch : Unit = {
    underlying.addBatch()
    rowCount += 1
    totalRowCount += 1

    if (rowCount % batchSize == 0) {
      executeBatch
    }
  }

  def finish : Unit = {
    if (rowCount > 0) {
      executeBatch
    }
  }

  private[oracle] def commit(): Unit = {
    if (conn != null && !conn.isClosed && !conn.getAutoCommit) {
      conn.commit()
    }
  }

  private[oracle] def abort(): Unit = {
    if (conn != null && !conn.isClosed) {
      conn.rollback()
    }
  }

  private[oracle] def close(): Unit = {
    if (conn != null && !conn.isClosed) {
      conn.close()
    }
  }

}
