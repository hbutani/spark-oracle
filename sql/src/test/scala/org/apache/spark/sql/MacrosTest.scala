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

package org.apache.spark.sql

import org.apache.spark.sql.hive.test.oracle.TestOracleHive
import org.apache.spark.sql.oracle.AbstractTest
import org.apache.spark.sql.sqlmacros.SQLMacroExpressionBuilder

class MacrosTest extends AbstractTest {

  import org.apache.spark.sql.defineMacros._
  import scala.tools.reflect.ToolBox
  import org.apache.spark.sql.catalyst.ScalaReflection._
  import universe._

  private val tb = mirror.mkToolBox()

  private def eval[A1, RT](fnTree : Tree) : Either[Function1[A1, RT], SQLMacroExpressionBuilder] = {
    tb.eval(
      q"""{
          new org.apache.spark.sql.defineMacros.SparkSessionMacroExt(
             org.apache.spark.sql.hive.test.oracle.TestOracleHive.sparkSession
             ).udm(${fnTree})
          }
        """).asInstanceOf[Either[Function1[A1, RT], SQLMacroExpressionBuilder]]
  }

  // scalastyle:off println
  private def handleMacroOutput[A1, RT](
      r: Either[Function1[A1, RT], SQLMacroExpressionBuilder]) = {
    r match {
      case Left(fn) => println(s"Failed to create expression for ${fn}")
      case Right(fb) => println(s"Spark SQL expression is ${fb.macroExpr.toString()}")
    }
  }
  // scalastyle:on

  test("basicMacros") { td =>
    handleMacroOutput(TestOracleHive.sparkSession.udm((i: Int) => i))

    handleMacroOutput(TestOracleHive.sparkSession.udm((i: Int) => {
      val j = 5
      j
    }))

    handleMacroOutput(TestOracleHive.sparkSession.udm((i: Int) => {
      val b = Array(5)
      val j = 5
      j
    }))

//    val a = Array(5)
//    handleMacroOutput(TestOracleHive.sparkSession.udm((i: Int) => {
//      val j = a(0)
//      j
//    }))

  }

  test("runtimeCompile") {td =>

    handleMacroOutput(eval[Int, Int](q"(i : Int) => i"))

    handleMacroOutput(eval[java.lang.Integer, java.lang.Integer](q"(i : java.lang.Integer) => i"))

//    val a = Array(5)
//    handleMacroOutput(eval[Int, Int](q"(i : Int) => a(0)"))

    handleMacroOutput(eval[Int, Int](q"(i : Int) => i + 5"))

    handleMacroOutput(eval[Int, Int](
      q"""{(i : Int) =>
          val b = Array(5)
          val j = 5
          j
        }"""))
  }

  test("macroPlan") { td =>

    TestOracleHive.sparkSession.registerMacro("fnM", {
      TestOracleHive.sparkSession.udm((i: Int) => i + 1)
    })

    TestOracleHive.udf.register("fn", (i: Int) => i + 1)

    val dfM = TestOracleHive.sql("select fnM(c_int) from sparktest.unit_test")
    println(
      s"""Macro based Plan:
         |${dfM.queryExecution.analyzed}""".stripMargin
    )

    val dfF = TestOracleHive.sql("select fn(c_int) from sparktest.unit_test")
    println(
      s"""Function based Plan:
         |${dfF.queryExecution.analyzed}""".stripMargin
    )

  }

}
