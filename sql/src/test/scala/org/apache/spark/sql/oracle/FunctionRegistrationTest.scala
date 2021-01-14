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

import org.apache.spark.sql.hive.test.oracle.TestOracleHive

abstract class FunctionRegistrationTest extends AbstractTest
  with PlanTestHelpers {

  val standard_funcs : Seq[AnyRef] = Seq(
    "ADD_MONTHS", "BITAND", "LAST_DAY",
    "MONTHS_BETWEEN", "NEXT_DAY", "REGEXP_COUNT", "REGEXP_INSTR",
    "REGEXP_REPLACE", "REGEXP_SUBSTR",
    "USER", ("SYS_CONTEXT", "ora_context")
    /* , "XOR"  we don't support pl_sql boolean type */
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    import org.apache.spark.sql.oracle._

    // scalastyle:off println
    println(
      TestOracleHive.sparkSession.registerOracleFunctions(Some("STANDARD"), standard_funcs : _*)
    )
    // scalastyle:on
  }


  test("register-and_query") { td =>

    val df = TestOracleHive.sql(
      """
        |select oracle.ADD_MONTHS(C_DATE, 1) add_months,
        |       oracle.bitand(c_short, c_int) bit_and,
        |       oracle.LAST_DAY(C_DATE) last_day,
        |       oracle.MONTHS_BETWEEN(oracle.NEXT_DAY(C_DATE, 'TUESDAY'),
        |       oracle.LAST_DAY(C_DATE)) mon_betw,
        |       oracle.user() ouser,
        |       oracle.ORA_CONTEXT('USERENV', 'CLIENT_PROGRAM_NAME') ora_client_pgm
        |from sparktest.unit_test
        |""".stripMargin
    )

    df.show()
  }

  test("pushdown-off") { td =>

    try {
      OraSparkConfig.setConf(OraSparkConfig.ENABLE_ORA_PUSHDOWN, false)
      val df = TestOracleHive.sql(
        """
          |select oracle.ADD_MONTHS(C_DATE, 1) add_months,
          |       oracle.bitand(c_short, c_int) bit_and,
          |       oracle.LAST_DAY(C_DATE) last_day,
          |       oracle.MONTHS_BETWEEN(oracle.NEXT_DAY(C_DATE, 'TUESDAY'),
          |       oracle.LAST_DAY(C_DATE)) mon_betw,
          |       oracle.user() ouser,
          |       oracle.ORA_CONTEXT('USERENV', 'CLIENT_PROGRAM_NAME') ora_client_pgm
          |from sparktest.unit_test
          |""".stripMargin
      )

      val ex: Exception = intercept[UnsupportedOperationException] {
        df.show()
      }
      println(ex.getMessage)
    } finally {
      OraSparkConfig.setConf(OraSparkConfig.ENABLE_ORA_PUSHDOWN, false)
    }

  }

}
