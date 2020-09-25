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

import org.apache.spark.sql.connector.write.{
  BatchWrite,
  SupportsDynamicOverwrite,
  SupportsOverwrite,
  SupportsTruncate,
  WriteBuilder
}
import org.apache.spark.sql.oracle.expressions.OraExpression
import org.apache.spark.sql.sources.Filter

case class OraWriteBuilder(writeSpec: OraWriteSpec)
    extends WriteBuilder
    with SupportsTruncate
    with SupportsOverwrite
    with SupportsDynamicOverwrite {

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    val oraExprs = filters.map(f => OraExpression.convert(f, writeSpec.oraTable))
    OraWriteBuilder(writeSpec.setDeleteFilters("TODO"))
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    OraWriteBuilder(writeSpec.setDynPartitionOverwrite)
  }

  override def buildForBatch(): BatchWrite = super.buildForBatch()

  override def truncate(): WriteBuilder = {
    OraWriteBuilder(writeSpec.setTruncate)
  }
}
