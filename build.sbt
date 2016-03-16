

name := "ADAMpro"

lazy val commonSettings = Seq(
  organization := "ch.unibas.dmi.dbis",
  version := "0.1.0",
  scalaVersion := "2.10.6"
)

//projects
lazy val root = (project in file(".")).
  settings(commonSettings : _*)

lazy val grpc = project.
  settings(commonSettings: _*)

lazy val client = project.
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

//libs
resolvers ++= Seq(
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)
resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  "org.apache.spark"       %%   "spark-core"             % "1.6.1",
  "org.apache.spark"       %%   "spark-sql"              % "1.6.1",
  "org.apache.spark"       %%   "spark-hive"             % "1.6.1",
  "org.apache.spark"       %%   "spark-mllib"            % "1.6.1",
  "org.scalanlp" 		       %%   "breeze" 				         % "0.11.2",
  "org.scalanlp" 		       %%   "breeze-natives" 	       % "0.11.2",
  "com.typesafe.slick"     %%   "slick"                  % "3.1.0",
  "com.h2database"         %    "h2"                     % "1.4.188",
  "org.postgresql"         %    "postgresql"             % "9.4-1201-jdbc41",
  "com.datastax.spark"     %%   "spark-cassandra-connector" % "1.6.0-M1",
  "com.google.guava"       %    "guava"                  % "19.0",
  "com.fasterxml.jackson.core" % "jackson-core"          % "2.4.4"
)

unmanagedBase <<= baseDirectory { base => base / "lib" }

//assembly
assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = false)

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.last
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.last
}

mainClass in assembly := Some("ch.unibas.dmi.dbis.adam.main.Startup")

//test
libraryDependencies ++= Seq(
  "org.scalatest"          % "scalatest_2.10"            %  "3.0.0-M15"
)

parallelExecution in Test := false
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

