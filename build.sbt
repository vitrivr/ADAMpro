name := "adamtwo"

version := "1.0"

scalaVersion := "2.10.6"

organization := "ch.unibas.dmi.dbis"


resolvers ++= Seq(
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "spray repo" at "http://repo.spray.io"
)
resolvers += Resolver.sonatypeRepo("snapshots")


//libs
libraryDependencies ++= Seq(
  "org.apache.spark"       %%   "spark-core"             % "1.5.2"   % "provided",
  "org.apache.spark"       %%   "spark-sql"              % "1.5.2" % "provided",
  "org.apache.spark"       %%   "spark-hive"             % "1.5.2",
  "org.apache.spark"       %%   "spark-mllib"            % "1.5.2" % "provided",
  "org.scalanlp" 		       %%   "breeze" 				         % "0.11.2",
  "org.scalanlp" 		       %%   "breeze-natives" 	       % "0.11.2",
  "io.spray"               %%   "spray-can"     		     % "1.3.3",
  "io.spray"               %%   "spray-routing" 		     % "1.3.3",
  "com.typesafe.slick"     %%   "slick"                  % "3.1.0",
  "com.h2database"         %    "h2"                     % "1.4.188",
  "org.postgresql"         %    "postgresql"             % "9.4-1201-jdbc41",
  "com.typesafe.akka"      %%   "akka-actor"             % "2.3.14",
  "org.json4s"             %%   "json4s-native"          % "3.3.0",
  "org.slf4j"              %    "slf4j-nop"              % "1.7.13",
  "org.iq80.leveldb"       %    "leveldb"                % "0.7",
  "com.datastax.spark"     %%  "spark-cassandra-connector" % "1.5.0-M3"
)


mainClass := Some("ch.unibas.dmi.dbis.adam.main.Startup")
unmanagedResourceDirectories in Compile += baseDirectory.value / "conf"

assemblyJarName in assembly := "adamtwo-assembly.jar"

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

//scalacOptions += "-Xlog-implicits"