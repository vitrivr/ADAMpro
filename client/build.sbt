name := "ADAMpro-client"

resolvers ++= Seq(
  "Twitter Maven" at "http://maven.twttr.com",
  "Finatra Repo" at "http://twitter.github.com/finatra"
)

libraryDependencies ++= Seq(
  "com.google.protobuf" % "protobuf-java" % "3.0.0-beta-2",
  "com.twitter.finatra" %% "finatra-http" % "2.1.5"
)

unmanagedBase <<= baseDirectory { base => base / ".." / "lib" }