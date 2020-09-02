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

import sbt.Keys.{baseDirectory, fullClasspath, resourceGenerators, version}
import sbt._
import sbtassembly.AssemblyPlugin.autoImport.{
  MergeStrategy,
  PathList,
  assemblyExcludedJars,
  assemblyMergeStrategy,
  assemblyOption
}
import sbtassembly.AssemblyKeys.assembly

import scala.sys.process.Process

object Assembly {

  def assemblyPredicate(d: Attributed[File]): Boolean = {
    true
  }

  lazy val assemblySettings =
    Seq(
      resourceGenerators in Compile += Def.task {
        /*
         * This is a hack.
         * path "/../../build/spark-oracle-build-info" hard codes structure of projects
         * that are assembled (packaging/spark_extend, packaging/spark_embed)
         */
        val buildScript = baseDirectory.value + "/../../build/spark-oracle-build-info"
        val targetDir = baseDirectory.value + "/target/extra-resources/"
        val command = Seq("bash", buildScript, targetDir, version.value)
        Process(command).!!
        val propsFile =
          baseDirectory.value / "target" / "extra-resources" / "spark-oracle-version-info.properties"
        Seq(propsFile)
      }.taskValue,
      assemblyOption in assembly :=
        (assemblyOption in assembly).value.copy(includeScala = false),
      assemblyExcludedJars in assembly := {
        val cp = (fullClasspath in assembly).value
        cp filter assemblyPredicate
      },
      assemblyMergeStrategy in assembly := {
        case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
        case PathList("META-INF", "maven", ps @ _*) => MergeStrategy.first
        case PathList("META-INF", "services", ps @ _*) => MergeStrategy.first
        case PathList("com", "fasterxml", "jackson", "annotation", _*) => MergeStrategy.first
        case PathList(ps @ _*) if ps.last == "pom.properties" => MergeStrategy.first
        case PathList(ps @ _*) if ps.last == "spark-oracle-version-info.properties" =>
          MergeStrategy.first
        case x =>
          val oldStrategy = (assemblyMergeStrategy in assembly).value
          oldStrategy(x)
      })

}
