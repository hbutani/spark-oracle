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
package org.apache.spark.sql.oracle

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.buildConf

object OraSparkConfig {

  val VARCHAR2_MAX_LENGTH =
    buildConf("spark.sql.oracle.max_string_size")
      .doc("Used as the varchar2 datatype length when translating string datatype. " +
        "Default is 4000")
      .intConf
      .createWithDefault(4000)

  val ENABLE_ORA_PUSHDOWN = buildConf(
    "spark.sql.oracle.enable.pushdown").
    doc("Enable Pushdown of Spark Operators as Oracle Queries").
    booleanConf.createWithDefault(true)

  val ENABLE_ORA_QUERY_SPLITTING = buildConf(
    "spark.sql.oracle.enable.querysplitting").
    doc("""Enable Splitting Oracle Pushdown Queries into multiple
    |queries, one per task. Currently default is false.
    |The process of inferring Query splits, runs an explain on
    |the pushdown query. This may incur an overhead of 100s of mSecs.
    |In situations where it is ok to always run 1 task or
    |when explain overhead is high, set to false.
    |Typically low latency queries return small amounts of data
    |so setting up a single task is reasonable.
    |In the future we will provide a mechanism for a
    |user to specify split strategy""".stripMargin
    ).
    booleanConf.
    createWithDefault(true)

  val BYTES_PER_SPLIT_TASK =
    buildConf("spark.sql.oracle.querysplit.target")
      .doc(
        """Split pushdown query so that each Task returns these many bytes
          |(specified in MB). Default is 1MB.
          |
          |But number of fetch tasks upper bounded by
          | `spark.sql.oracle.querysplit.maxfetch.rounds`""".stripMargin)
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(JavaUtils.byteStringAs("1MB", ByteUnit.BYTE))

  val MAX_SPLIT_FETCH_TASKS =
    buildConf("spark.sql.oracle.querysplit.maxfetch.rounds")
      .doc(
        """We will split the fetching into tasks based on the
          |`spark.sql.oracle.querysplit.target`, but the maximum number of fetch tasks
          |will be Math.floor(`this_value` * default_parallelism) of the cluster.
          |
          |So suppose `spark.sql.oracle.querysplit.target`=1MB; the fetch size is 100MB, the
          |defaultParallelism=8, and this_value is 1. That means even though according to the
          |querysplit.target setting we should have 100 tasks; we will be restricted to
          | 8 tasks(= 8 * 1)
          |""".stripMargin)
      .doubleConf
      .createWithDefault(1.0)

  val ALLOW_SPLITBY_RESULTSET = buildConf(
    "spark.sql.oracle.allow.splitresultset").
    doc(
      """Many queries that cannot be split by rows/partitions
        |of one of the tables scanned in  the query.
        |Most such queries will return small amout of data, so no
        |splitting is needed. But when a large amount of data
        |is returned by such a query, a possible splitting strategy
        |is to split by resultset using ` OFFSET ? ROWS FETCH NEXT ? ROWS ONLY` clause.
        |But query tasks issue the same query with different fetch batches,
        |potentially of different jdbc connections may strain the
        |server. So we allow for result based splitting to be turned off
        |By default it is turned on.""".stripMargin
    ).
    booleanConf.
    createWithDefault(true)

  def getConf[T](configEntry : ConfigEntry[T])(
    implicit sparkSession : SparkSession = OraSparkUtils.currentSparkSession
  ) : T = {
    getConf(configEntry, sparkSession.sqlContext.conf)
  }

  def getConf[T](configEntry : ConfigEntry[T],
                 conf: SQLConf) : T = {
    conf.getConf(configEntry)
  }

  def setConf[T](configEntry : ConfigEntry[T], value : T)(
    implicit sparkSession : SparkSession = OraSparkUtils.currentSparkSession
  ) : Unit = {
    setConf(configEntry, value, sparkSession.sqlContext.conf)
  }

  def setConf[T](configEntry : ConfigEntry[T],
                 value : T,
                 conf: SQLConf) : Unit = {
    conf.setConf(configEntry, value)
  }
}
