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

import org.apache.spark.sql.connector.catalog.oracle.OraMetadataMgrInternalTest
import org.apache.spark.sql.hive.test.oracle.TestOracleHive

// scalastyle:off println
class BasicReadTest extends AbstractTest with OraMetadataMgrInternalTest {

  def showPlan(sqlStat: String): Unit = {
    println("regular explain:")
    TestOracleHive.sql(s"explain ${sqlStat}").show(false)

    if (false) { // logical plan throws unsupported parse in spark
      println("logical explain:")
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
    println("formatted explain:")
    TestOracleHive.sql(s"explain formatted ${sqlStat}").show(false)

    println("extended explain:")
    TestOracleHive.sql(s"explain extended ${sqlStat}").show(false)

    /*
    Other explain forms available in Spark:

    println("codegen explain:")
    TestOracleHive.sql(s"explain codegen ${sqlStat}").show(false)

    println("cost explain:")
    TestOracleHive.sql(s"explain cost ${sqlStat}").show(false)
   */
  }

  test("select") { td =>
    showPlan("""
        |select SS_SOLD_DATE_SK, SS_SOLD_TIME_SK, SS_ITEM_SK
        |from store_sales""".stripMargin)
  }

  test("partFilter") { td =>
    showPlan("""
        |select SS_SOLD_DATE_SK, SS_SOLD_TIME_SK, SS_ITEM_SK * 5 - 50
        |from store_sales
        |where SS_SOLD_DATE_SK > 2451058
        |""".stripMargin)
  }

  test("dataFilter") { td =>
    showPlan("""
        |select SS_SOLD_DATE_SK, SS_SOLD_TIME_SK, SS_ITEM_SK * 5 - 50
        |from store_sales
        |where SS_SOLD_DATE_SK > 2451058
        |""".stripMargin)
  }

  test("partAndDataFilter") { td =>
    showPlan("""
        |select SS_SOLD_DATE_SK, SS_SOLD_TIME_SK, SS_ITEM_SK * 5 - 50
        |from store_sales
        |where SS_LIST_PRICE > 0 and  SS_QUANTITY > 0 and SS_SOLD_DATE_SK > 2451058
        |""".stripMargin)
  }

  test("join") { td =>
    showPlan("""
               |select SS_SOLD_DATE_SK, SS_SOLD_TIME_SK, SS_ITEM_SK * 5 - 50, C_FIRST_NAME
               |from store_sales, customer
               |where SS_LIST_PRICE > 0 and  SS_QUANTITY > 0 and SS_SOLD_DATE_SK > 2451058
               |      and SS_CUSTOMER_SK = C_CUSTOMER_SK
               |      and C_BIRTH_YEAR > 2000
               |""".stripMargin)
  }

}
