name := "ADAMpro-evaluation"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.5",
  "org.apache.logging.log4j" % "log4j-api" % "2.5",
  "com.google.protobuf" % "protobuf-java" % "3.0.0-beta-2",
  "net.liftweb" %% "lift-json" % "2.6"
)

unmanagedBase <<= baseDirectory { base => base / ".." / "lib" }

//assembly
assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = true)