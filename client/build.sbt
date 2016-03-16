name := "ADAMpro-client"

libraryDependencies ++= Seq(
)

unmanagedBase <<= baseDirectory { base => base / ".." / "lib" }