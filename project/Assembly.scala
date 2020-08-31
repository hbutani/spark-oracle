import sbt.Keys.{baseDirectory, fullClasspath, resourceGenerators, version}
import sbt._
import sbtassembly.AssemblyPlugin.autoImport.{MergeStrategy, PathList, assemblyExcludedJars, assemblyMergeStrategy, assemblyOption}
import sbtassembly.AssemblyKeys.assembly

import scala.sys.process.Process

object Assembly {

  def assemblyPredicate(d : Attributed[File]) : Boolean = {
    true
  }

  lazy val assemblySettings = Seq(
    resourceGenerators in Compile += Def.task {
      val buildScript = baseDirectory.value + "/build/spark-oracle-build-info"
      val targetDir = baseDirectory.value + "/target/extra-resources/"
      val command = Seq("bash", buildScript, targetDir, version.value)
      Process(command).!!
      val propsFile =
        baseDirectory.value / "target" / "extra-resources" / "snap-version-info.properties"
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
      case PathList("com", "fasterxml", "jackson" ,"annotation", _*) => MergeStrategy.first
      case PathList(ps @ _*) if ps.last == "pom.properties" => MergeStrategy.first
      case PathList(ps @ _*) if ps.last == "spark-oracle-version-info.properties" => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

}
