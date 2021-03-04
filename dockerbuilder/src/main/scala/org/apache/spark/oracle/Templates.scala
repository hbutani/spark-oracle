/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.apache.spark.oracle

object Templates {

  object Interpreter_Json {

    val template = "zeppelin.interpreter.json.template"
    val outFileName = "interpreter.json"

    val spark_cores = "$$spark.driver.cores$$"
    val spark_mem = "$$spark.driver.memory$$"
    val spark_app_name = "$$spark.app.name$$"
    val spark_ver_check = "$$zeppelin.spark.enableSupportedVersionCheck$$"

    // TODO set SPARK_HOME
    def replacementMap(cfg : Config) : Map[String, String] = {
      Map(
        spark_cores -> cfg.spark_cores.toString,
        spark_mem -> cfg.spark_mem.toString,
        spark_app_name -> "Spark_Oracle_Demo",
        spark_ver_check -> "false"
      )
    }
  }

}
