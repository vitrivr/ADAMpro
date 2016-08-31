//protobuf
import com.trueaccord.scalapb.{ScalaPbPlugin => PB}
import sbtassembly.AssemblyPlugin.autoImport._

name := "ADAMpro-grpc"

import com.trueaccord.scalapb.{ScalaPbPlugin => PB}

PB.protobufSettings

//keep this part for Jenkins
PB.runProtoc in PB.protobufConfig := (args =>
  com.github.os72.protocjar.Protoc.runProtoc("-v300" +: args.toArray))
version in PB.protobufConfig := "3.0.0"


resolvers ++= Seq(
  "Twitter" at "http://maven.twttr.com/"
)

libraryDependencies ++= Seq(
  "io.grpc" % "grpc-all" % "1.0.0",
  "com.trueaccord.scalapb" %% "scalapb-runtime-grpc" % (PB.scalapbVersion in PB.protobufConfig).value
)

//assembly
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("io.netty.**" -> "adampro.grpc.shaded.io.netty.@1").inAll,
  ShadeRule.rename("com.google.**" -> "adampro.grpc.shaded.com.google.@1").inAll,
  ShadeRule.rename("com.fasterxml.**" -> "adampro.grpc.shaded.com.fasterxml.@1").inAll,
  ShadeRule.rename("org.apache.**" -> "adampro.grpc.shaded.org.apache.@1").inAll
)

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