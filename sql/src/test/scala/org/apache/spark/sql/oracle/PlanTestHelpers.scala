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

import java.io.PrintStream

import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.operators.OraTableScanValidator
import org.apache.spark.sql.oracle.operators.OraTableScanValidator.ScanDetails

trait PlanTestHelpers {

  // scalastyle:off println
  def showPlan(sqlStat: String, out: PrintStream = System.out): Unit = {
    out.println("regular explain:")
    TestOracleHive.sql(s"explain ${sqlStat}").show(false)

    if (false) { // logical plan throws unsupported parse in spark
      out.println("logical explain:")
      TestOracleHive.sql(s"explain logical ${sqlStat}")
    }

    /*
     * Reading the formatted output of BatchScan:
     * (1) BatchScan
     *    Output [3]     :  for example => [SS_SOLD_TIME_SK#45, SS_ITEM_SK#46, SS_SOLD_DATE_SK#44]
     *    Format         : orafile
     *    OraPlan        : 00 OraTableScan toString on OraTable (see below for example)
     *    PartitionSchema: struct<SS_SOLD_DATE_SK:decimal(38,18)>
     *    ReadSchema     : for example => struct<SS_SOLD_TIME_SK:decimal(38,18),SS_ITEM_SK:decimal(38,18)>
     *    dsKey          : for example => DataSourceKey(jdbc:oracle:thin:@mammoth_medium,tpcds)
     *
     * OraPlan output:
     * - based on oraPlan.numberedTreeString call
     * - so OraTableScan operator outputs args that are not None
     *   - oraTable, catalystOp, catalystOutputSchema, projections, filter, partitionFilter
     * - Example:
     *    OraTable(TPCDS,STORE_SALES,[Lorg.apache.spark.sql.connector.catalog.oracle.OracleMetadata$OraColumn;@60660d21,Some(TablePartitionScheme([Ljava.lang.String;@2898c70d,RANGE,None)),[Lorg.apache.spark.sql.connector.catalog.oracle.OracleMetadata$OraTablePartition;@ec67be1,None,[Lorg.apache.spark.sql.connector.catalog.oracle.OracleMetadata$OraForeignKey;@64a00fe0,false,TableStats(Some(1573492),Some(8192),Some(287997024),Some(102.0)),Map()), {SS_SOLD_DATE_SK#67, SS_SOLD_TIME_SK#68, SS_ITEM_SK#69},
     *     [DummyOraExpression SS_SOLD_DATE_SK#67: decimal(38,18)01 , DummyOraExpression SS_SOLD_TIME_SK#68: decimal(38,18)2 , DummyOraExpression SS_ITEM_SK#69: decimal(38,18)3 ]
     */
    out.println("formatted explain:")
    TestOracleHive.sql(s"explain formatted ${sqlStat}").show(false)

    out.println("extended explain:")
    TestOracleHive.sql(s"explain extended ${sqlStat}").show(false)

    /*
    Other explain forms available in Spark:

    println("codegen explain:")
    TestOracleHive.sql(s"explain codegen ${sqlStat}").show(false)

    println("cost explain:")
    TestOracleHive.sql(s"explain cost ${sqlStat}").show(false)
   */
  }

  /**
   * Use this method to get the [[ScanDetails]] in a Plan
   * Build the `reqdScans` for the [[validateOraScans]] method from this output.
   *
   * [[Literal]] values in the output have to be fixed.
   * For example convert: `Literal(2451058.000000000000000000)` to `Literal(Decimal(0E-18, 38, 18)`
   *
   * @param sqlStat
   * @param out
   */
  def showOraScans(sqlStat: String, out: PrintStream = System.out): Unit = {
    val plan = TestOracleHive.sql(sqlStat).queryExecution.optimizedPlan
    val scanV = OraTableScanValidator(plan)
    scanV.dumpTableScans(out)
  }

  def validateOraScans(sqlStat: String, reqdScans: Map[String, ScanDetails]): Unit = {
    val plan = TestOracleHive.sql(sqlStat).queryExecution.optimizedPlan
    val scanV = OraTableScanValidator(plan)
    scanV.validateScans(reqdScans)
  }
  // scalastyle:on println

}
