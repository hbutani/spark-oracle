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

  test("compileTime") { td =>
    handleMacroOutput(TestOracleHive.sparkSession.udm((i: Int) => i))

    handleMacroOutput(TestOracleHive.sparkSession.udm((i: Int) => i + 1))

    handleMacroOutput(TestOracleHive.sparkSession.udm((i: Int) => {
      val j = 5
      j
    }))

    handleMacroOutput(TestOracleHive.sparkSession.udm((i: Int) => {
      val b = Array(5)
      val j = 5
      j
    }))
  }

  test("basics") {td =>

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

    handleMacroOutput(eval[Int, Int](reify {
      (i : Int) => org.apache.spark.SPARK_BRANCH.length + i
    }.tree))

    handleMacroOutput(eval[Int, Int](reify {(i : Int) =>
      val b = Array(5, 6)
      val j = b(0)
      i + j + Math.abs(j)}.tree))

    handleMacroOutput(eval[Int, Int](
      q"""{(i : Int) =>
          val b = Array(5)
          val j = 5
          j
        }"""))
  }

  test("udts") {td =>
    import macrotest.ExampleStructs.Point
    handleMacroOutput(eval[Point, Point](
      reify {(p : Point) =>
          Point(1, 2)
        }.tree
    )
    )

    handleMacroOutput(eval[Point, Int](
      reify {(p : Point) =>
        p.x + p.y
      }.tree
    )
    )

    handleMacroOutput(eval[Point, Int](
      reify {(p : Point) =>
        Point(p.x + p.y, p.y)
      }.tree
    )
    )
  }

  test("optimizeExpr") { td =>
    import macrotest.ExampleStructs.Point
    handleMacroOutput(eval[Point, Int](
      reify {(p : Point) =>
        val p1 = Point(p.x, p.y)
        val a = Array(1)
        val m = Map(1 -> 2)
        p1.x + p1.y + a(0) + m(1)
      }.tree
    )
    )
  }

  test("tuples") {td =>
    handleMacroOutput(eval[Tuple2[Int, Int], Tuple2[Int, Int]](
      reify {(t : Tuple2[Int, Int]) =>
        (t._2, t._1)
      }.tree
    )
    )

    handleMacroOutput(eval[Tuple2[Int, Int], Tuple2[Int, Int]](
      reify {(t : Tuple2[Int, Int]) =>
        t._2 -> t._1
      }.tree
    )
    )

    handleMacroOutput(eval[Tuple4[Float, Double, Int, Int], Tuple2[Int, Int]](
      reify {(t : Tuple4[Float, Double, Int, Int]) =>
        (t._4 + t._3, t._4)
      }.tree
    )
    )
  }

  test("arrays") {td =>
    handleMacroOutput(eval[Int, Int](
      reify {(i : Int) =>
          val b = Array(5, i)
          val j = b(0)
          j + b(1)
        }.tree))
  }

  test("maps") {td =>
    handleMacroOutput(eval[Int, Int](
      reify {(i : Int) =>
          val b = Map(0 -> i, 1 -> (i + 1))
          val j = b(0)
          j + b(1)
        }.tree))
  }

  test("datetimes") {td =>
    import java.sql.Date
    import java.sql.Timestamp
    import java.time.ZoneId
    import java.time.Instant
    import org.apache.spark.unsafe.types.CalendarInterval
    import org.apache.spark.sql.sqlmacros.DateTimeUtils._

    handleMacroOutput(eval[Date, Int](
      reify {(dt : Date) =>
        val dtVal = dt
        val dtVal2 = new Date(System.currentTimeMillis())
        val tVal = new Timestamp(System.currentTimeMillis())
        val dVal3 = localDateToDays(java.time.LocalDate.of(2000, 1, 1))
        val t2 = instantToMicros(Instant.now())
        val t3 = stringToTimestamp("2000-01-01", ZoneId.systemDefault()).get
        val t4 = daysToMicros(dtVal, ZoneId.systemDefault())
        getDayInYear(dtVal) + getDayOfMonth(dtVal) + getDayOfWeek(dtVal2) +
          getHours(tVal, ZoneId.systemDefault) + getSeconds(t2, ZoneId.systemDefault) +
          getMinutes(t3, ZoneId.systemDefault()) +
          getDayInYear(dateAddMonths(dtVal, getMonth(dtVal2))) +
          getDayInYear(dVal3) +
          getHours(
            timestampAddInterval(t4, new CalendarInterval(1, 1, 1), ZoneId.systemDefault()),
            ZoneId.systemDefault) +
          getDayInYear(dateAddInterval(dtVal, new CalendarInterval(1, 1, 1L))) +
          monthsBetween(t2, t3, true, ZoneId.systemDefault()) +
          getDayOfMonth(getNextDateForDayOfWeek(dtVal2, "MO")) +
          getDayInYear(getLastDayOfMonth(dtVal2)) + getDayOfWeek(truncDate(dtVal, "week")) +
          getHours(toUTCTime(t3, ZoneId.systemDefault().toString), ZoneId.systemDefault())
      }.tree))
  }

  test("macroVsFuncPlan") { td =>

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

  test("macroPlan") { td =>

    import TestOracleHive.sparkSession.implicits._

    TestOracleHive.sparkSession.registerMacro("m1",
      TestOracleHive.sparkSession.udm(
        {(i : Int) =>
          val b = Array(5, 6)
          val j = b(0)
          val k = new java.sql.Date(System.currentTimeMillis()).getTime
          i + j + k + Math.abs(j)
        }
      )
    )

    val dfM = TestOracleHive.sql("select m1(c_int) from sparktest.unit_test")
    println(
      s"""Macro based Plan:
         |${dfM.queryExecution.analyzed}""".stripMargin
    )

  }

}
