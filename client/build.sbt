name := "ADAMpro-client"

resolvers ++= Seq(
  "Twitter Maven" at "http://maven.twttr.com",
  "Finatra Repo" at "http://twitter.github.com/finatra"
)

libraryDependencies ++= Seq(
  "com.google.protobuf" % "protobuf-java" % "3.0.0-beta-2",
  "com.twitter.finatra" %% "finatra-http" % "2.1.5",
  "org.slf4j" % "slf4j-simple" % "1.7.21"
)

unmanagedBase <<= baseDirectory { base => base / ".." / "lib" }

//assembly
assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = true)

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

mainClass in assembly := Some("ch.unibas.dmi.dbis.adam.client.main.ClientStartup")

test in assembly := {}