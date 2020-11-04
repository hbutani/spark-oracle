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

class UncorrelatedSubQueryTranslationTests extends AbstractTranslationTest {
  testPushdown("inSubQuery",
    """
      |select c_long
      |from sparktest.unit_test
      |where c_int in (select c_int
      |                from sparktest.unit_test_partitioned
      |                where c_int > 5
      |                )
      |""".stripMargin
  )

  testPushdown("notinSubQuery",
    """
      |select c_long
      |from sparktest.unit_test
      |where c_int not in (select c_int
      |                from sparktest.unit_test_partitioned
      |                where c_int > 5
      |                )
      |""".stripMargin
  )

  testPushdown("existsSubQuery",
    """
      | with ssales as
      | (select ss_item_sk
      | from store_sales
      | where ss_customer_sk = 8
      | ),
      | ssales_other as
      | (select ss_item_sk
      | from store_sales
      | where ss_customer_sk = 10
      | )
      | select ss_item_sk
      | from ssales
      | where exists (select ssales_other.ss_item_sk
      |               from ssales_other
      |
      |               )
      |""".stripMargin
  )

  testPushdown("notExistsSubQuery",
    """
      | with ssales as
      | (select ss_item_sk
      | from store_sales
      | where ss_customer_sk = 8
      | ),
      | ssales_other as
      | (select ss_item_sk
      | from store_sales
      | where ss_customer_sk = 10
      | )
      | select ss_item_sk
      | from ssales
      | where not exists (select ssales_other.ss_item_sk
      |               from ssales_other
      |               )
      |""".stripMargin
  )
}
