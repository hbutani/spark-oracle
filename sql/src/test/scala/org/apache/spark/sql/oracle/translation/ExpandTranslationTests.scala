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
package org.apache.spark.sql.oracle.translation

// scalastyle:off line.size.limit println
class ExpandTranslationTests extends AbstractTranslationTest {

    testPushdown("distinct",
      """select c_int as ci, c_long as cl,
        |       sum(distinct c_decimal_scale_8) + count(distinct c_decimal_scale_5)
        |from sparktest.unit_test
        |group by  c_int + c_long, c_int, c_long
      """.stripMargin,
      """select "5_sparkora", "C_INT", "C_LONG", "6_sparkora", "7_sparkora", "gid"
        |from SPARKTEST.UNIT_TEST   , lateral ( select ("C_INT" + "C_LONG") "5_sparkora", "C_DECIMAL_SCALE_5" "6_sparkora", null "7_sparkora", 1 "gid" from dual union all select ("C_INT" + "C_LONG"), null, "C_DECIMAL_SCALE_8", 2 from dual )""".stripMargin
    )

  testPushdown("rollup",
    """
      |select i_category
      |                  ,d_year
      |                  ,d_qoy
      |                  ,d_moy
      |                  ,s_store_id
      |                  ,sum(ss_sales_price*ss_quantity) sumsales
      |            from store_sales
      |                ,date_dim
      |                ,store
      |                ,item
      |       where  ss_sold_date_sk=d_date_sk
      |          and ss_item_sk=i_item_sk
      |          and ss_store_sk = s_store_sk
      |          and d_month_seq between 1200 and 1200+11
      |       group by  rollup(i_category, d_year, d_qoy, d_moy,s_store_id)
      |""".stripMargin,
    """select "SS_QUANTITY", "SS_SALES_PRICE", "i_category", "d_year", "d_qoy", "d_moy", "s_store_id", "spark_grouping_id"
      |from TPCDS.STORE_SALES  join TPCDS.DATE_DIM  on ("SS_SOLD_DATE_SK" = "D_DATE_SK") join TPCDS.STORE  on ("SS_STORE_SK" = "S_STORE_SK") join TPCDS.ITEM  on ("SS_ITEM_SK" = "I_ITEM_SK")  , lateral ( select "I_CATEGORY" "i_category", "D_YEAR" "d_year", "D_QOY" "d_qoy", "D_MOY" "d_moy", "S_STORE_ID" "s_store_id", 0 "spark_grouping_id" from dual union all select "I_CATEGORY", "D_YEAR", "D_QOY", "D_MOY", null, 1 from dual union all select "I_CATEGORY", "D_YEAR", "D_QOY", null, null, 3 from dual union all select "I_CATEGORY", "D_YEAR", null, null, null, 7 from dual union all select "I_CATEGORY", null, null, null, null, 15 from dual union all select null, null, null, null, null, 31 from dual )
      |where (("SS_STORE_SK" IS NOT NULL AND "SS_SOLD_DATE_SK" IS NOT NULL) AND (("D_MONTH_SEQ" IS NOT NULL AND ("D_MONTH_SEQ" >= ?)) AND ("D_MONTH_SEQ" <= ?)))""".stripMargin
  )

  testPushdown("cube1",
    """select c_int as ci, c_long as cl,
      |       sum(c_decimal_scale_8) + count(c_decimal_scale_5)
      |from sparktest.unit_test
      |group by  cube(c_int + c_long, c_int, c_long)
      """.stripMargin,
    """select "C_DECIMAL_SCALE_5", "C_DECIMAL_SCALE_8", "6_sparkora", "c_int", "c_long", "spark_grouping_id"
      |from ( select "C_DECIMAL_SCALE_5", "C_DECIMAL_SCALE_8", ("C_INT" + "C_LONG") AS "1_sparkora", "C_INT", "C_LONG"
      |from SPARKTEST.UNIT_TEST  )   , lateral ( select "1_sparkora" "6_sparkora", "C_INT" "c_int", "C_LONG" "c_long", 0 "spark_grouping_id" from dual union all select "1_sparkora", "C_INT", null, 1 from dual union all select "1_sparkora", null, "C_LONG", 2 from dual union all select "1_sparkora", null, null, 3 from dual union all select null, "C_INT", "C_LONG", 4 from dual union all select null, "C_INT", null, 5 from dual union all select null, null, "C_LONG", 6 from dual union all select null, null, null, 7 from dual )""".stripMargin
  )

  testPushdown("cube2",
    """
      |select i_category
      |                  ,d_year + d_qoy
      |                  ,s_store_id
      |                  ,sum(ss_sales_price*ss_quantity) sumsales
      |            from store_sales
      |                ,date_dim
      |                ,store
      |                ,item
      |       where  ss_sold_date_sk=d_date_sk
      |          and ss_item_sk=i_item_sk
      |          and ss_store_sk = s_store_sk
      |          and d_month_seq between 1200 and 1200+11
      |       group by  cube(i_category, d_year + d_qoy, s_store_id)""".stripMargin,
    """select "SS_QUANTITY", "SS_SALES_PRICE", "i_category", "6_sparkora", "s_store_id", "spark_grouping_id"
      |from ( select "SS_QUANTITY", "SS_SALES_PRICE", "I_CATEGORY", ("D_YEAR" + "D_QOY") AS "1_sparkora", "S_STORE_ID"
      |from TPCDS.STORE_SALES  join TPCDS.DATE_DIM  on ("SS_SOLD_DATE_SK" = "D_DATE_SK") join TPCDS.STORE  on ("SS_STORE_SK" = "S_STORE_SK") join TPCDS.ITEM  on ("SS_ITEM_SK" = "I_ITEM_SK")
      |where (("SS_STORE_SK" IS NOT NULL AND "SS_SOLD_DATE_SK" IS NOT NULL) AND (("D_MONTH_SEQ" IS NOT NULL AND ("D_MONTH_SEQ" >= ?)) AND ("D_MONTH_SEQ" <= ?))) )   , lateral ( select "I_CATEGORY" "i_category", "1_sparkora" "6_sparkora", "S_STORE_ID" "s_store_id", 0 "spark_grouping_id" from dual union all select "I_CATEGORY", "1_sparkora", null, 1 from dual union all select "I_CATEGORY", null, "S_STORE_ID", 2 from dual union all select "I_CATEGORY", null, null, 3 from dual union all select null, "1_sparkora", "S_STORE_ID", 4 from dual union all select null, "1_sparkora", null, 5 from dual union all select null, null, "S_STORE_ID", 6 from dual union all select null, null, null, 7 from dual )""".stripMargin
      )
}
