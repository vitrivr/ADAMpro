name := "ADAMpro-chronos"

libraryDependencies ++= Seq(
  "com.google.protobuf" % "protobuf-java" % "3.1.0",
  "io.grpc" % "grpc-netty" % "1.2.0",
  "io.grpc" % "grpc-protobuf" % "1.2.0",
  "io.grpc" % "grpc-stub" % "1.2.0",
  "org.slf4j" % "slf4j-simple" % "1.7.21",
  "org.scala-lang.modules" %% "scala-xml" % "1.0.6"
)

unmanagedBase <<= baseDirectory { base => base / ".." / "lib" }

//assembly
assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = true)

val metaMime = """META.INF(.)mime.types""".r
val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.last
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case metaMime(_) => MergeStrategy.deduplicate
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.last
}

test in assembly := {}