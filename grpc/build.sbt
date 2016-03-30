//protobuf
import com.trueaccord.scalapb.{ScalaPbPlugin => PB}
import sbtassembly.AssemblyPlugin.autoImport._

name := "ADAMpro-grpc"

import com.trueaccord.scalapb.{ScalaPbPlugin => PB}

PB.protobufSettings

resolvers ++= Seq(
  "Twitter" at "http://maven.twttr.com/"
)

libraryDependencies ++= Seq(
  "io.grpc" % "grpc-all" % "0.13.1",
  "com.trueaccord.scalapb" %% "scalapb-runtime-grpc" % (PB.scalapbVersion in PB.protobufConfig).value,
  "com.fasterxml.jackson.core" % "jackson-core"          % "2.4.4"
)

//assembly
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("io.netty.**" -> "shaded.io.netty.@1").inAll,
  ShadeRule.rename("com.google.**" -> "shaded.com.google.@1").inAll,
  ShadeRule.rename("com.fasterxml.**" -> "shaded.com.fasterxml.@1").inAll
)

assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case x => MergeStrategy.first
}