name := "adamtwo"

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
  "org.apache.spark"       %%   "spark-core"             % "1.6.0",
  "org.apache.spark"       %%   "spark-sql"              % "1.6.0",
  "org.apache.spark"       %%   "spark-hive"             % "1.6.0",
  "org.apache.spark"       %%   "spark-mllib"            % "1.6.0",
  "org.scalanlp" 		       %%   "breeze" 				         % "0.11.2",
  "org.scalanlp" 		       %%   "breeze-natives" 	       % "0.11.2",
  "com.typesafe.slick"     %%   "slick"                  % "3.1.0",
  "com.h2database"         %    "h2"                     % "1.4.188",
  "org.postgresql"         %    "postgresql"             % "9.4-1201-jdbc41",
  "org.iq80.leveldb"       %    "leveldb"                % "0.7",
  "com.datastax.spark"     %%   "spark-cassandra-connector" % "1.6.0-M1",
  "com.google.guava"       %    "guava"                  % "19.0"
)

unmanagedBase <<= baseDirectory { base => base / "lib" }

//assembly
assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = false)

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}