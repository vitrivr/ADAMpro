name := "ADAMpro-client"

libraryDependencies ++= Seq(
  "com.google.protobuf" % "protobuf-java" % "3.0.0-beta-2"
)

unmanagedBase <<= baseDirectory { base => base / ".." / "lib" }