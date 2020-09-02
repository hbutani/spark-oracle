import Dependencies._
import sbt.Keys.test
import sbt._

ThisBuild / scalaVersion := Versions.scalaVersion
ThisBuild / crossScalaVersions := Seq(Versions.scalaVersion)

ThisBuild / homepage := Some(url("https://orahub.oci.oraclecorp.com/harish_butani/spark-oracle"))
ThisBuild / licenses := List("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / organization := "com.oracle.spark"
ThisBuild / organizationName := "Oracle"
ThisBuild / version := Versions.sparkOracleVersion

// from https://www.scala-sbt.org/1.x/docs/Cached-Resolution.html
// added to commonSettings
// ThisBuild / updateOptions := updateOptions.value.withLatestSnapshots(false)
// ThisBuild / updateOptions := updateOptions.value.withCachedResolution(true)

Global / resolvers ++= Seq(
  DefaultMavenRepository,
  Resolver.sonatypeRepo("public"),
  "Apache snapshots repo" at "https://repository.apache.org/content/groups/snapshots/")

lazy val commonSettings = Seq(
  updateOptions := updateOptions.value.withLatestSnapshots(false),
  updateOptions := updateOptions.value.withCachedResolution(true),
  javaOptions := Seq(
    "-Xms1g",
    "-Xmx3g",
    "-Duser.timezone=UTC",
    "-Dscalac.patmat.analysisBudget=512",
    "-XX:MaxPermSize=256M"),
  scalacOptions ++= Seq("-target:jvm-1.8", "-feature", "-deprecation"),
  test in assembly := {},
  fork in Test := false,
  parallelExecution in Test := false,
  libraryDependencies ++= (scala.dependencies ++
    spark.dependencies ++
    utils.dependencies ++
    test_infra.dependencies))

lazy val test_support = project
  .in(file("test_support"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings: _*)

lazy val common = project
  .in(file("common"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings: _*)
  .dependsOn(test_support % "test")

lazy val orastuff = project
  .in(file("orastuff"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= oracle.dependencies)
  .dependsOn(test_support % "test", common)

lazy val sql = project
  .in(file("sql"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings: _*)
  .dependsOn(test_support % "test", common, orastuff)

lazy val mllib = project
  .in(file("mllib"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings: _*)
  .dependsOn(test_support % "test", common, orastuff)

lazy val spark_extend = project
  .in(file("packaging/spark_extend"))
  .enablePlugins(UniversalPlugin)
  .settings(commonSettings: _*)
  .settings(Assembly.assemblySettings: _*)
  // remove root folder; set jar name; add maintainer
  .settings(
    maintainer in Universal := "harish.butani@oracle.com",
    packageName in Universal := "spark-oracle-" + (version.value),
    topLevelDirectory in Universal := None,
    mappings in Universal += {
      val assemblyJar = (assembly).value
      assemblyJar -> ("jars/" + assemblyJar.getName)
    })
  .dependsOn(test_support % "test", common, orastuff, sql, mllib)

lazy val spark_embed = project
  .in(file("packaging/spark_embed"))
  .settings(commonSettings: _*)
  .settings(Assembly.assemblySettings: _*)
  .dependsOn(test_support % "test", common, orastuff, sql, mllib)
