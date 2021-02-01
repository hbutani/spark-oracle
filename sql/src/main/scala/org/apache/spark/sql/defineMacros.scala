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

import scala.language.experimental.macros
import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.functions._
import org.apache.spark.sql.sqlmacros.{SQLMacro, _}

// scalastyle:off
object defineMacros {
// scalastyle:on

  // scalastyle:off line.size.limit

  class SparkSessionMacroExt(val sparkSession: SparkSession) extends AnyVal {

    def udm[RT, A1](f: Function1[A1, RT]) :
    Either[Function1[A1, RT], SQLMacroExpressionBuilder] = macro SQLMacro.udm1_impl[RT, A1]

    def registerMacro[RT : TypeTag, A1 : TypeTag](nm : String,
                                                  udm : Either[Function1[A1, RT], SQLMacroExpressionBuilder]
                                                 ) : Unit = {
      udm match {
        case Left(fn) =>
          sparkSession.udf.register(nm, udf(fn))
        case Right(sqlMacroBldr) =>
          sparkSession.sessionState.functionRegistry.createOrReplaceTempFunction(nm, sqlMacroBldr)
      }
    }
  }

  implicit def ssWithMacros(ss : SparkSession) : SparkSessionMacroExt = new SparkSessionMacroExt(ss)

}
