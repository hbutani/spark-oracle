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

// scalastyle:off line.size.limit
class JoinTranslationTests extends AbstractTranslationTest {

  testPushdown("join_4way",
    """
      |select a.c_int, b.c_int, c.c_int, d.c_int
      |from sparktest.unit_test a,
      |     sparktest.unit_test_partitioned b,
      |     sparktest.unit_test c,
      |     sparktest.unit_test_partitioned d
      |where a.c_int = b.c_int and b.c_int = c.c_int and c.c_int = d.c_int""".stripMargin
  )

  testPushdown("join_4way_with_aliases",
    """
      |select a.c_int, b.c_int, c.c_int, d.c_int
      |from (select c_int + 1 as c_int from sparktest.unit_test) a,
      |     (select c_int + 1 as c_int from sparktest.unit_test_partitioned) b,
      |     (select c_int + 1 as c_int from sparktest.unit_test) c,
      |     (select c_int + 1 as c_int from sparktest.unit_test_partitioned) d
      |where a.c_int = b.c_int and b.c_int = c.c_int and c.c_int = d.c_int""".stripMargin
  )

  testPushdown("l_outer_q5",
    """
      |select ws_web_site_sk as wsr_web_site_sk,
      |           wr_returned_date_sk as date_sk,
      |           ws_sales_price as sales_price,
      |           ws_net_profit as profit,
      |           wr_return_amt as return_amt,
      |           wr_net_loss as net_loss
      |    from web_returns left outer join web_sales on
      |         ( wr_item_sk = ws_item_sk
      |           and wr_order_number = ws_order_number)
      |           """.stripMargin
  )

  testPushdown("full_outer_51",
  """select case when web.ws_item_sk is not null then web.ws_item_sk else store.ss_item_sk end item_sk
    |                 ,case when web.ws_sold_date_sk is not null then web.ws_sold_date_sk else store.ss_sold_date_sk end d_date
    |                 ,web.ws_sales_price web_sales
    |                 ,store.ss_sales_price store_sales
    |           from web_sales web full outer join store_sales store on (web.ws_item_sk = store.ss_item_sk
    |                                                          and web.ws_sold_date_sk = store.ss_sold_date_sk)""".stripMargin)

}
