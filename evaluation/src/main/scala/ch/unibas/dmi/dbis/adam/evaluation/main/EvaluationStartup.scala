package ch.unibas.dmi.dbis.adam.evaluation.main

import ch.unibas.dmi.dbis.adam.evaluation.grpc.RPCClient

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object EvaluationStartup {
  //val grpcHost = "10.34.58.136"
  val grpcHost = "localhost"
  val grpcPort = 5890

  def main(args: Array[String]): Unit = {
    System.out.println("Starting Evaluation on " + grpcHost + ":" + grpcPort)
    RPCClient(grpcHost, grpcPort)
  }
}
