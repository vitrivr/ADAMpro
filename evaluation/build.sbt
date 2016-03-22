name := "ADAMpro-evaluation"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.5",
  "org.apache.logging.log4j" % "log4j-api" % "2.5"
)

unmanagedBase <<= baseDirectory { base => base / ".." / "lib" }