import sbt._
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

name := "ADAMpro"

lazy val commonSettings = Seq(
  organization := "org.vitrivr",
  version := "0.1.0",
  scalaVersion := "2.11.7"
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
  scalaVersion := "2.11.7",
  crossScalaVersions := Seq("2.11.7"),
  ivyScala := ivyScala.value.map(_.copy(overrideScalaVersion = true))
)

mainClass in(Compile, run) := Some("org.vitrivr.adampro.main.Startup")

unmanagedResourceDirectories in Compile += baseDirectory.value / "conf"
scalacOptions ++= Seq()

//lib resolvers
resolvers ++= Seq(
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Restlet Repositories" at "http://maven.restlet.org"
)
resolvers += Resolver.sonatypeRepo("snapshots")

//base libs
val baseLibs = Seq(
  "org.scala-lang" % "scala-compiler" % "2.11.7",
  "org.scala-lang" % "scala-reflect" % "2.11.7"
)

//adampro core libs
val coreLibs = Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" excludeAll ExclusionRule("org.apache.hadoop"), //make sure that you use the same spark version as in your deployment!
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-hive" % "2.1.0",
  "org.apache.spark" %% "spark-mllib" % "2.1.0",
  "org.apache.hadoop" % "hadoop-client" % "2.7.0" excludeAll ExclusionRule("javax.servlet") //make sure that you use the same hadoop version as in your deployment!
).map(
  _.excludeAll(
    ExclusionRule("org.scala-lang")
  )
)
//TODO: add multiple configurations to sbt, one which has coreLibs as provided (as they do not have to be submitted to spark)

//secondary libs
val secondaryLibs = Seq(
  "org.scalanlp" %% "breeze" % "0.11.2",
  "org.scalanlp" %% "breeze-natives" % "0.11.2",
  "com.typesafe.slick" %% "slick" % "3.1.1",
  "com.mchange" % "c3p0" % "0.9.5.2",
  "org.apache.derby" % "derby" % "10.13.1.1",
  "it.unimi.dsi" % "fastutil" % "7.0.12",
  "org.apache.commons" % "commons-lang3" % "3.4",
  "org.apache.commons" % "commons-math3" % "3.4.1",
  "com.googlecode.javaewah" % "JavaEWAH" % "1.1.6",
  "com.google.guava" % "guava" % "19.0",
  "com.google.protobuf" % "protobuf-java" % "3.0.0" % "protobuf",
  "com.trueaccord.scalapb" %% "scalapb-runtime" % com.trueaccord.scalapb.compiler.Version.scalapbVersion % "protobuf",
  "io.grpc" % "grpc-protobuf" % "1.0.1",
  "org.jgrapht" % "jgrapht-core" % "1.0.0"
).map(
  _.excludeAll(
    ExclusionRule("org.scala-lang"),
    ExclusionRule("org.slf4j"),
    ExclusionRule("log4j")
  )
)


//tertiary libs
val tertiaryLibs = Seq(
  "com.lucidworks.spark" % "spark-solr" % "3.0.0-alpha.2",
  "org.postgresql" % "postgresql" % "9.4.1208",
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0-M3",
  "net.postgis" % "postgis-jdbc" % "2.2.1",
  "com.databricks" %% "spark-avro" % "3.1.0"
).map(
  _.excludeAll(
    ExclusionRule("org.scala-lang"),
    ExclusionRule("org.slf4j"),
    ExclusionRule("log4j")
  )
)

//test libs
val testLibs = Seq(
  "org.scalatest" %% "scalatest" % "3.0.0",
  "io.grpc" % "grpc-netty" % "1.0.1"
).map(
  _.excludeAll(
    ExclusionRule("org.scala-lang"),
    ExclusionRule("org.slf4j"),
    ExclusionRule("log4j")
  )
)

libraryDependencies := baseLibs ++ coreLibs ++ secondaryLibs ++ tertiaryLibs ++ testLibs

unmanagedBase <<= baseDirectory { base => base / "lib" }
unmanagedResourceDirectories in Compile += baseDirectory.value / "conf"

//assembly
assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = true)

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case x if x.contains("slf4j-api") => MergeStrategy.last
  case x if x.contains("org.slf4j") => MergeStrategy.last
  case x if x.contains("org.apache.httpcomponents") => MergeStrategy.last
  case x if x.contains("org.apache.commons") => MergeStrategy.last
  case x if x.contains("org.apache.derby") => MergeStrategy.last
  case PathList("application.conf") => MergeStrategy.discard
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.last
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.last
}

assemblyShadeRules in assembly := Seq(
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