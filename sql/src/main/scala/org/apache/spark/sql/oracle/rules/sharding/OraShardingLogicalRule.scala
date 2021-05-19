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

package org.apache.spark.sql.oracle.rules.sharding

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.oracle.OracleCatalog
import org.apache.spark.sql.oracle.rules.OraLogicalRule

abstract class OraShardingLogicalRule extends OraLogicalRule {

  override protected def isRewriteEnabled(implicit sparkSession: SparkSession): Boolean = {
    val oraCatalog =
      sparkSession.sessionState.catalogManager.catalog("oracle").asInstanceOf[OracleCatalog]

    if (oraCatalog.getMetadataManager.isSharded) {
      import org.apache.spark.sql.oracle.OraSparkConfig._
      super.isRewriteEnabled && getConf(ENABLE_SHARD_INSTANCE_PUSHDOWN)(sparkSession)
    } else {
      false
    }
  }
}
