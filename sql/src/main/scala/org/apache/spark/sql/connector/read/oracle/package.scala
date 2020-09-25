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

package org.apache.spark.sql.connector.read

/**
 * This package contains the structures and functions for ''read'' plans.
 *
 * '''Read Path Planning flow:'''
 * <img src="doc-files/readPathPlanning.png" />
 *
 * '''Read Execution:'''
 * <img src="doc-files/readExecution.png" />
 *
 * [[OraScanBuilder]]:
 *
 *  - responsible for setting up an [[OraScan]]
 *  - for an [[OracleTable]] with optional filter pushdowns and `requiredSchema`
 *    it sets up a [[OraPlan]], that is passed to the [[OraScan]].
 *    - it is not required for the [[OraPlan]] to apply all filters, as these are applied on
 *      top of the [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation]]
 *      Ensuring these can be pushed to Oracle will be done in the ''Oracle pushdown rules''.
 *
 * [[OraScan]] :
 *
 *  - acts like a [[FileScan]], so the
 *   [[org.apache.spark.sql.execution.datasources.PruneFileSourcePartitions]]
 *    rule can apply on this scan, and ''partition'' and ''data'' filter
 *    expressions can be pushed to it.
 *  - but implementation behavior is completely overridden.
 *  - it has an empty `fileIndex`
 *  - it reports `partitionFilters` and `dataFilters` to be empty.
 *    The filters pushed into the [[OraPlan]] are reapplied on top of the
 *    [[org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation]]
 *  - For physical planning:
 *    - it uses [[OraQuerySplitting]] to infer how to parallelize the [[OraPlan]]
 *    - each [[OracleDBSplit]] has an enhanced [[OraPlan]].
 *    - An [[OraPartition]] is setup for each Split with its oracle query, bind values and
 *     preferred locations.
 *  - stats estimation: try to use a table's stats otherwise estimate as unknown
 */
package object oracle {}
