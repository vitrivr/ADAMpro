import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

name := "ADAMpro"

lazy val commonSettings = Seq(
  organization := "ch.unibas.dmi.dbis",
  version := "0.1.0",
  scalaVersion := "2.10.6"
)

//projects
lazy val root = (project in file(".")).
  settings(commonSettings : _* )

lazy val grpc = project.
  settings(commonSettings ++ Seq(assemblyOutputPath in assembly := baseDirectory.value / ".." / "lib" / "grpc-assembly-0.1-SNAPSHOT.jar") : _*)

lazy val client = project.
  settings(commonSettings: _*)

lazy val evaluation = project.
  settings(commonSettings: _*)


//build
lazy val buildSettings = Seq(
  scalaVersion := "2.10.6",
  crossScalaVersions := Seq("2.10.6"),
  ivyScala := ivyScala.value.map(_.copy(overrideScalaVersion = true))
)

mainClass in (Compile, run) := Some("ch.unibas.dmi.dbis.adam.main.Startup")

unmanagedResourceDirectories in Compile += baseDirectory.value / "conf"
//scalacOptions += "-Xlog-implicits"
scalacOptions += "-target:jvm-1.7"

//libs
resolvers ++= Seq(
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)
resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  "org.apache.spark"       %%   "spark-core"             % "1.6.1" % "provided" excludeAll(
    ExclusionRule("org.apache.hadoop")
    ),
  "org.apache.spark"       %%   "spark-sql"              % "1.6.1" % "provided",
  "org.apache.spark"       %%   "spark-hive"             % "1.6.1" % "provided",
  "org.apache.spark"       %%   "spark-mllib"            % "1.6.1" % "provided",
  "org.scalanlp" 		       %%   "breeze" 				         % "0.11.2",
  "org.scalanlp" 		       %%   "breeze-natives" 	       % "0.11.2",
  "com.typesafe.slick"     %%   "slick"                  % "3.1.0",
  "com.h2database"         %    "h2"                     % "1.4.188",
  "org.postgresql"         %    "postgresql"             % "9.4.1208",
  "com.datastax.spark"     %%   "spark-cassandra-connector" % "1.6.0-M1",
  "com.fasterxml.jackson.core" % "jackson-core"          % "2.4.4",
  "org.apache.hadoop"      %    "hadoop-client"          % "2.6.4" % "provided",
  "org.apache.commons"     %    "commons-lang3"          % "3.4",
  "org.slf4j"              %     "slf4j-log4j12"         % "1.7.21",
  "it.unimi.dsi"           %    "fastutil"               % "7.0.12"
)

unmanagedBase <<= baseDirectory { base => base / "lib" }
unmanagedResourceDirectories in Compile += baseDirectory.value / "conf"

//assembly
assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = false)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll //different guava versions
)

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("application.conf") => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.last
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.last
}

mainClass in assembly := Some("ch.unibas.dmi.dbis.adam.main.Startup")

test in assembly := {}

//provided libraries should be included in "run"
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))


//test
libraryDependencies ++= Seq(
  "org.scalatest"          % "scalatest_2.10"            %  "3.0.0-M15"
)

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

lazy val runADAM = taskKey[Unit]("Runs ADAMpro in docker container.")
runADAM := {
  //TODO: check that docker is running
  assembly.value
  "./scripts/docker-runADAM.sh" !
}