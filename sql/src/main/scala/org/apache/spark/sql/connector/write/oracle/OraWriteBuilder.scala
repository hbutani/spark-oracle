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

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.oracle.expressions.DataSourceFilterTranslate
import org.apache.spark.sql.sources.Filter

case class OraWriteBuilder(writeSpec: OraWriteSpec)
    extends WriteBuilder
    with SupportsTruncate
    with SupportsOverwrite
    with SupportsDynamicOverwrite {

  /*
   * Called in non-dynamic partition mode from OverwriteByExpressionExec
   * when deleteCond is not just `true`
   */
  override def overwrite(filters: Array[Filter]): WriteBuilder = {

    val oraExpr =
      DataSourceFilterTranslate(filters, writeSpec.oraTable).oraExpression

    if (!oraExpr.isDefined) {
      throw new UnsupportedOperationException(
        s"""Delete condition filters: ${filters.mkString("[", ",", "]")}
           |cannot translate to oracle delete expression""".stripMargin
      )
    }

    OraWriteBuilder(writeSpec.setDeleteFilters(oraExpr.get))
  }

  /*
   * Called in dynamic partition mode from OverwritePartitionsDynamicExec
   */
  override def overwriteDynamicPartitions(): WriteBuilder = {
    OraWriteBuilder(writeSpec.setDynPartitionOverwriteMode)
  }

  override def buildForBatch(): BatchWrite = OraBatchWrite(writeSpec)

  /*
   * Called in non-dynamic partition mode from OverwriteByExpressionExec
   * when deleteCond is just `true`
   */
  override def truncate(): WriteBuilder = {
    OraWriteBuilder(writeSpec.setTruncate)
  }

}
