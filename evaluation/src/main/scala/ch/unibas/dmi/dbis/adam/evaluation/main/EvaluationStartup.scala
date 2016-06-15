package ch.unibas.dmi.dbis.adam.evaluation.main

import ch.unibas.dmi.dbis.adam.evaluation.grpc.RPCClient

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object  EvaluationStartup {
  val grpcHost = "localhost"
  val grpcPort = 5890

  def main(args: Array[String]): Unit = {
    val grpc: RPCClient = RPCClient(grpcHost, grpcPort)
  }
}
