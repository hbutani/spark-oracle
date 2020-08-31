import sbt.{ModuleID, _}

object Dependencies {
  import Versions._

  object scala {
    val dependencies = Seq(
      "org.scala-lang.modules" %% "scala-xml" % scalaXMLVersion % "provided",
      "org.scala-lang" % "scala-compiler" % scalaVersion % "provided",
      "org.scala-lang" % "scala-reflect" % scalaVersion % "provided",
      "org.scala-lang.modules" %% "scala-parser-combinators" % scalaParseCombVersion % "provided"
    )
  }

  object spark {
    val dependencies = Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-repl" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-unsafe" % sparkVersion % "provided"
    )
  }

  object utils {
    val dependencies = Seq(
      "org.json4s" %% "json4s-jackson" % json4sVersion % "provided",
      "org.slf4j" % "slf4j-api" % slf4jVersion % "provided",
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided",
      "org.slf4j" % "jul-to-slf4j" % slf4jVersion % "provided",
      "org.slf4j" % "jcl-over-slf4j" % slf4jVersion % "provided",
      "log4j" % "log4j" % log4jVersion % "provided"
    )
  }

  object oracle {
    val dependencies = Seq(
      "com.oracle.database.jdbc" % "ojdbc8" % oraVersion,
      "com.oracle.database.jdbc" % "ucp" % oraVersion,
      "com.oracle.database.security" % "oraclepki" % oraVersion,
      "com.oracle.database.security" % "osdt_cert" % oraVersion,
      "com.oracle.database.security" % "osdt_core" % oraVersion
    )
  }

  object test_infra {
    val dependencies = Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion % "test",
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "test",
      "org.apache.derby" % "derby" % derbyVersion %   "test"
    )
  }
}