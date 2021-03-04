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

import java.io.File

import scopt.OParser

object DockerBuilder {

  def log(s : Any) : Unit = {
    // scalastyle:off println
    print(s.toString)
    // scalastyle:on println
  }

  def run(cfg : Config) : Unit = {
    import cfg._

    log("\nSetting up configuration\n")

    log(s"Downloading spark from ${spark_download_url}\n")
    Utils.downloadURL(spark_download_url, s => log(s))

    log("\n")

    log(s"Downloading zeppelin from ${zeppelin_download_url}\n")
    log(s"   this can take some time\n")
    Utils.downloadURL(zeppelin_download_url, s => log(s))

    log("\n")
    log("Setup zeppelin interpreter.json\n")
    Utils.generateFromTemplate(Templates.Interpreter_Json.template,
      Templates.Interpreter_Json.replacementMap(cfg),
      new File(Templates.Interpreter_Json.outFileName)
    )
  }

  def main(args : Array[String]) : Unit = {
    OParser.parse(Config.parser, args, Config()) match {
      case Some(config) =>
      log(config)
        run(config)
      case _ =>
      // arguments are bad, error message will have been displayed
    }
  }

}
