import sbt.project
import sbt._
import sbt.ExclusionRule
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

name := "ADAMpro"

lazy val commonSettings = Seq(
  organization := "org.vitrivr",
  version := "0.1.0",
  scalaVersion := "2.10.6"
)

//projects
lazy val root = (project in file(".")).
  settings(commonSettings: _*)

lazy val grpc = project.
  settings(commonSettings ++ Seq(assemblyOutputPath in assembly := baseDirectory.value / ".." / "lib" / "grpc-assembly-0.1-SNAPSHOT.jar"): _*)

lazy val grpcclient = project.
  settings(commonSettings: _*)

lazy val chronos = project.dependsOn(grpcclient).
  settings(commonSettings: _*)

lazy val web = project.dependsOn(grpcclient).
  settings(commonSettings: _*)

lazy val importer = project.dependsOn(grpcclient).
  settings(commonSettings: _*)


//build
lazy val buildSettings = Seq(
  scalaVersion := "2.10.6",
  crossScalaVersions := Seq("2.10.6"),
  ivyScala := ivyScala.value.map(_.copy(overrideScalaVersion = true))
)

mainClass in(Compile, run) := Some("org.vitrivr.adampro.main.Startup")

unmanagedResourceDirectories in Compile += baseDirectory.value / "conf"
scalacOptions ++= Seq("-target:jvm-1.7")

//lib resolvers
resolvers ++= Seq(
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Restlet Repositories" at "http://maven.restlet.org"
)
resolvers += Resolver.sonatypeRepo("snapshots")

//base libs
val baseLibs = Seq(
  "org.scala-lang" % "scala-compiler" % "2.10.6",
  "org.scala-lang" % "scala-reflect" % "2.10.6"
)

//adampro core libs
val coreLibs = Seq(
  "org.apache.spark" %% "spark-core" % "1.6.3" excludeAll ExclusionRule("org.apache.hadoop"), //make sure that you use the same spark version as in your deployment!
  "org.apache.spark" %% "spark-sql" % "1.6.3",
  "org.apache.spark" %% "spark-hive" % "1.6.3",
  "org.apache.spark" %% "spark-mllib" % "1.6.3",
  "org.apache.hadoop" % "hadoop-client" % "2.7.0" excludeAll ExclusionRule("javax.servlet") //make sure that you use the same hadoop version as in your deployment!
).map(
  _.excludeAll(
    ExclusionRule("org.scala-lang"),
    ExclusionRule("org.slf4j", "slf4j-api")
  )
)
//TODO: add multiple configurations to sbt, one which has coreLibs as provided (as they do not have to be submitted to spark)

//secondary libs
val secondaryLibs = Seq(
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  "com.typesafe.slick" %% "slick" % "3.1.1",
  "c3p0" % "c3p0" % "0.9.1.2",
  "org.apache.derby" % "derby" % "10.10.2.0",
  "it.unimi.dsi" % "fastutil" % "7.0.12",
  "org.apache.commons" % "commons-lang3" % "3.4",
  "org.apache.commons" % "commons-math3" % "3.4.1",
  "com.googlecode.javaewah" % "JavaEWAH" % "1.1.6",
  "com.google.guava" % "guava" % "19.0",
  "com.google.protobuf" % "protobuf-java" % "3.0.0",
  "org.jgrapht" % "jgrapht-core" % "1.0.0"
).map(
  _.excludeAll(
    ExclusionRule("org.scala-lang"),
    ExclusionRule("org.slf4j", "slf4j-api")
  )
)

//log libs
val logLibs = Seq(
  "org.slf4j" % "slf4j-api" % "1.7.10",
  "org.slf4j" % "slf4j-log4j12" % "1.7.10"
)

//tertiary libs
val tertiaryLibs = Seq(
  "com.lucidworks.spark" % "spark-solr" % "2.1.0",
  "org.postgresql" % "postgresql" % "9.4.1208",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.2",
  "com.basho.riak" % "spark-riak-connector" % "1.5.1",
  "net.postgis" % "postgis-jdbc" % "2.2.1",
  "org.iq80.leveldb" % "leveldb" % "0.9",
  "com.databricks" %% "spark-avro" % "2.0.1"
).map(
  _.excludeAll(
    ExclusionRule("org.scala-lang"),
    ExclusionRule("org.slf4j", "slf4j-api")
  )
)

//test libs
val testLibs = Seq(
  "org.scalatest" % "scalatest_2.10" % "3.0.0"
).map(
  _.excludeAll(
    ExclusionRule("org.scala-lang"),
    ExclusionRule("org.slf4j", "slf4j-api")
  )
)

libraryDependencies := baseLibs ++ coreLibs ++ secondaryLibs ++ logLibs ++ tertiaryLibs ++ testLibs

unmanagedBase <<= baseDirectory { base => base / "lib" }
unmanagedResourceDirectories in Compile += baseDirectory.value / "conf"

//assembly
assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = false)

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case x if x.contains("slf4j-api") => MergeStrategy.last
  case x if x.contains("org.slf4j") => MergeStrategy.first
  case x if x.contains("org.apache.httpcomponents") => MergeStrategy.last
  case x if x.contains("org.apache.commons") => MergeStrategy.last
  case PathList("application.conf") => MergeStrategy.discard
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.last
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.last
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "adampro.root.shaded.com.google.protobuf.@1").inAll
)

mainClass in assembly := Some("org.vitrivr.adampro.main.Startup")

test in assembly := {}

//provided libraries should be included in "run"
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

parallelExecution in Test := false
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

//custom commands
addCommandAlias("proto", "; grpc/assembly")

lazy val setupDocker = taskKey[Unit]("Setup docker containers for ADAMpro to run.")
setupDocker := {
  "./scripts/docker-setup.sh" !
}

lazy val destroyDocker = taskKey[Unit]("Destroys docker containers for ADAMpro (careful: this command deletes the containers!).")
destroyDocker := {
  "./scripts/docker-destroy.sh" !
}

lazy val startDocker = taskKey[Unit]("Starts the docker containers for ADAMpro.")
startDocker := {
  "./scripts/docker-start.sh" !
}

lazy val stopDocker = taskKey[Unit]("Stops the docker containers for ADAMpro.")
stopDocker := {
  "./scripts/docker-stop.sh" !
}

lazy val runDocker = taskKey[Unit]("Runs ADAMpro in docker container.")
runDocker := {
  //TODO: check that docker is running before running assembly and submitting
  assembly.value
  "./scripts/docker-run.sh" !
}

lazy val buildDocker = taskKey[Unit]("Builds the image of a self-contained docker container.")
buildDocker := {
  assembly.in(web).value
  assembly.value
  "./scripts/docker-build.sh" !
}

lazy val runLocal = taskKey[Unit]("Runs ADAMpro locally (Apache Spark must be installed).")
runLocal := {
  assembly.value
  "./scripts/runLocal.sh" !
}