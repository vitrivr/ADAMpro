name := "adamtwo-client"

libraryDependencies ++= Seq(
)

unmanagedBase <<= baseDirectory { base => base / ".." / "lib" }