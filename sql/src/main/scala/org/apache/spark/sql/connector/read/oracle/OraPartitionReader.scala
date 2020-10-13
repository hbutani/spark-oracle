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
package org.apache.spark.sql.connector.read.oracle

import java.sql.{PreparedStatement, ResultSet}

import oracle.spark.ConnectionManagement
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, SpecificInternalRow}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.connector.read.oracle.OraPartition.OraQueryAccumulators
import org.apache.spark.sql.oracle.OracleCatalogOptions
import org.apache.spark.sql.oracle.expressions.OraLiterals.{jdbcGetSet, JDBCGetSet}
import org.apache.spark.sql.oracle.sqlexec.SparkOraStatement
import org.apache.spark.util.{DoubleAccumulator, NextIterator}

case class OraPartitionReader(
    oraPart: OraPartition,
    catalystOutput: Seq[Attribute],
    accumulators: OraQueryAccumulators)
    extends PartitionReader[InternalRow]
    with Logging { self =>

  private[this] var closed = false

  private[this] val oraQuery = OraQueryStatement(oraPart, accumulators.timeToFirstRow)

  import oraQuery.{underlying => stmt, conn}

  private[this] val rs: ResultSet = oraQuery.executeQuery()

  val itr = new NextIterator[InternalRow] {

    private[this] val getters: Seq[JDBCGetSet[_]] =
      catalystOutput.map { attr =>
        jdbcGetSet(attr.dataType)
      }
    private[this] val mutableRow = new SpecificInternalRow(catalystOutput.map(x => x.dataType))

    override protected def getNext(): InternalRow = {
      if (rs.next()) {
        accumulators.rowsRead.add(1L)
        var i = 0
        while (i < getters.length) {
          getters(i).readValue(rs, i, mutableRow)
          i = i + 1
        }
        mutableRow
      } else {
        finished = true
        null.asInstanceOf[InternalRow]
      }
    }

    override def close(): Unit = self.close

  }

  override def next(): Boolean = itr.hasNext

  override def get(): InternalRow = itr.next()

  override def close(): Unit = {
    if (closed) return
    try {
      if (null != rs) {
        rs.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing resultset", e)
    }
    try {
      if (null != stmt) {
        stmt.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing statement", e)
    }
    try {
      if (null != conn) {
        if (!conn.isClosed && !conn.getAutoCommit) {
          try {
            conn.commit()
          } catch {
            case NonFatal(e) => logWarning("Exception committing transaction", e)
          }
        }
        conn.close()
      }
      logInfo("closed connection")
    } catch {
      case e: Exception => logWarning("Exception closing connection", e)
    }
    closed = true
  }

}

case class OraQueryStatement(oraPart: OraPartition, timeToExecute: DoubleAccumulator)
    extends SparkOraStatement {

  import oraPart._

  lazy val conn = ConnectionManagement.getConnectionInExecutor(dsInfo)

  lazy val sqlTemplate: String = sqlSnippet.sql
  lazy val bindValues: Seq[Literal] = sqlSnippet.params

  lazy val underlying: PreparedStatement = {
    val ps =
      conn.prepareStatement(sqlTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    ps.setFetchSize(dsInfo.catalogOptions.fetchSize)
    bindValues(ps)
    ps
  }

  override def catalogOptions: OracleCatalogOptions = dsInfo.catalogOptions

  private def bindValues(ps: PreparedStatement): Unit = {
    val setters: Seq[JDBCGetSet[_]] =
      bindValues.map { lit =>
        jdbcGetSet(lit.dataType)
      }
    for (((bV, setter), i) <- bindValues.zip(setters).zipWithIndex) {
      setter.setValue(bV, ps, i)
    }
  }
}
