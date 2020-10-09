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

package org.apache.spark.sql.oracle.sqlexec

import java.sql.PreparedStatement

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.oracle.{LoggingAndTimingSQL, OracleCatalogOptions}

/**
 * Represent oracle jdbc statements issued from Spark Executors.
 * Provides Logging and Error tracking.
 * Based on StatementExecutor from scalikejdc and
 * [[org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils]]
 */
trait SparkOraStatement extends Logging { self =>
  import SparkOraStatement._

  val underlying: PreparedStatement
  val sqlTemplate: String
  val bindValues: Seq[Literal]
  val catalogOptions: OracleCatalogOptions

  private[this] val statementExecute = new Do with LogSQLAndTiming with LogSQLFailure

  def executeQuery(): java.sql.ResultSet = statementExecute(() => underlying.executeQuery())

  def executeUpdate(): Int = statementExecute(() => underlying.executeUpdate())

  private[this] lazy val sqlString: String = ???

  private[this] def stackTraceInformation: String = ???

  private trait LogSQLFailure extends Execute {
    abstract override def apply[A](fn: () => A): A =
      try {
        super.apply(fn)
      } catch {
        case e: Exception =>
          logError(s"""SQL execution failed (Reason: ${e.getMessage} )
             |SQL is:
             |${sqlString}
             |Stack Trace:
             |${stackTraceInformation}
             |""".stripMargin)
          throw e
      }
  }

  private trait LogSQLAndTiming extends Execute {
    abstract override def apply[A](fn: () => A): A = {
      val before = System.currentTimeMillis
      val result = super.apply(fn)
      val after = System.currentTimeMillis
      val spentMillis = after - before

      val logIt = catalogOptions.logSQLBehavior.enabled ||
        (catalogOptions.logSQLBehavior.enableSlowSqlWarn &&
          catalogOptions.logSQLBehavior.slowSqlThreshold < spentMillis)

      if (logIt) {
        val logLevel = if (catalogOptions.logSQLBehavior.enabled) {
          catalogOptions.logSQLBehavior.logLevel
        } else {
          catalogOptions.logSQLBehavior.slowSqlLogLevel
        }

        def msg: String = {
          s"""SQL execution completed in ${spentMillis} millis
             |SQL is:
             |${sqlString}
             |""".stripMargin
        }
        import LoggingAndTimingSQL.LogLevel
        LoggingAndTimingSQL.LogLevel(logLevel) match {
          case LogLevel.INFO => self.logInfo(msg)
          case LogLevel.DEBUG => self.logDebug(msg)
          case LogLevel.WARNING => self.logWarning(msg)
          case LogLevel.ERROR => self.logError(msg)
          case LogLevel.TRACE => self.logTrace(msg)
        }

      }

      result
    }
  }

}

object SparkOraStatement {

  private trait Execute {
    def apply[A](fn: () => A): A
  }

  private class Do extends Execute {
    override def apply[A](fn: () => A): A = fn()
  }
}
