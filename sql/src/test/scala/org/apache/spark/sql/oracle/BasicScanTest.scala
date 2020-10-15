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

/**
 * Running this test when on VPN is extremely slow.
 * See issue described in [[oracle.jdbc.internal.ResultSetCache:ensureSAXParserFactorySet]]
 */
class BasicScanTest extends AbstractTest with PlanTestHelpers {

  test(
    "ship_mode_scan",
    """
      |select sm_ship_mode_sk, sm_ship_mode_id,
      |       sm_type, sm_code, sm_carrier,
      |       sm_contract
      |from ship_mode""".stripMargin,
    true,
    true)

  test(
    "call_center",
    """
      |select CC_CALL_CENTER_ID, CC_REC_START_DATE,
      |       CC_NAME, CC_CLASS, CC_EMPLOYEES, CC_SQ_FT,
      |       CC_HOURS,
      |       CC_STREET_NUMBER,
      |       CC_SUITE_NUMBER, CC_CITY, CC_COUNTY, CC_STATE,
      |       CC_ZIP, CC_GMT_OFFSET, CC_TAX_PERCENTAGE
      |from call_center
      |where CC_STATE='MI'""".stripMargin,
    true,
    true)

  test(
    "store_sales_scan",
    """
      |select SS_SOLD_DATE_SK, SS_SOLD_TIME_SK, SS_ITEM_SK,
      |       SS_TICKET_NUMBER, SS_QUANTITY,
      |       SS_WHOLESALE_COST, SS_LIST_PRICE,
      |       SS_COUPON_AMT, SS_NET_PAID, SS_NET_PAID_INC_TAX, SS_NET_PROFIT
      |from store_sales
      |where SS_LIST_PRICE > 185 and  SS_QUANTITY > 99
      |      and SS_SOLD_DATE_SK = 2451058""".stripMargin,
    true,
    true)
}
