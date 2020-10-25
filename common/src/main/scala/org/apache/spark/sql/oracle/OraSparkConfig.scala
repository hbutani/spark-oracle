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

  def getConf[T](configEntry : ConfigEntry[T])(
    implicit sparkSession : SparkSession = OraSparkUtils.currentSparkSession
  ) : T = {
    getConf(configEntry, sparkSession.sqlContext.conf)
  }

  def getConf[T](configEntry : ConfigEntry[T],
                 conf: SQLConf) : T = {
    conf.getConf(configEntry)
  }
}
