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
class WindowTranslationTests extends AbstractTranslationTest {

  testPushdown("SUM",
    """select SUM(ws_ext_sales_price) from web_sales
      """.stripMargin
  )

  testPushdown("Col Name Resolution",
    """|SELECT SUM(ws_ext_sales_price) OVER
      |         (PARTITION BY ws_bill_customer_sk ORDER BY ws_item_sk)
      |         AS ws
      |  FROM web_sales limit 100
      """.stripMargin
  )

  testPushdown("Col Name Resolution",
    """|SELECT SUM(ws_ext_sales_price) OVER
       |         (PARTITION BY ws_bill_customer_sk)
       |         AS ws
       |  FROM web_sales limit 100
      """.stripMargin
  )
}
