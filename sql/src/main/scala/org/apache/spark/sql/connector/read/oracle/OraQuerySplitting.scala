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

import oracle.hcat.db.split.OracleDBSplit
import oracle.spark.DataSourceKey

import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.oracle.operators.OraPlan

object OraQuerySplitting {

  /**
   * Issue an explain plan on the query.
   * Inspect the plan based on ideas here
   * in [[https://orahub.oci.oraclecorp.com/harish_butani/spark-oracle/wikis/Data-Movement]]
   * to come up with splits. For now splits represented as [[OracleDBSplit]]
   *
   * Based on the splitting chosen, we can also specify the [[Partitioning]].
   *
   * @param dsKey
   * @param oraPlan
   * @return
   */
  def generateSplits(
      dsKey: DataSourceKey,
      oraPlan: OraPlan): (Array[OracleDBSplit], Partitioning) = {
    // TODO
    (Array.empty, OraUnknownDistribution(0))
  }

  /**
   * [[OracleDBSplit]] implies a predicate and optionally a partition clause
   * to be applied to an Oracle query. In the [[OraPlan]] this should be
   * applied to the scan operator of the table specified in the split.
   *
   * @param oraPlan
   * @param dbSplit
   * @return
   */
  def applySplit(oraPlan: OraPlan, dbSplit: OracleDBSplit): OraPlan = {
    // TODO
    oraPlan
  }

  /**
   * A split potentially implies a block or partition restriction.
   * This plus
   * - some way to associate block/partitions to physical hosts
   * - some environment information that associates spark nodes with oracle nodes
   * can be used to some up with `preferred locations` for a split.
   *
   * @param dbSplit
   * @return
   */
  def preferedLocations(dbSplit: OracleDBSplit): Array[String] = {
    // TODO
    Array.empty
  }

}
