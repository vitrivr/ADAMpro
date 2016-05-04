package ch.unibas.dmi.dbis.adam.client.main

import ch.unibas.dmi.dbis.adam.client.grpc.RPCClient
import ch.unibas.dmi.dbis.adam.client.web.{AdamController, WebServer}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object ClientStartup {
  val httpPort = 9099

  val grpcHost = "p2.cs.unibas.ch"
  val grpcPort = 5890

  def main(args: Array[String]): Unit = {
    val grpc = RPCClient(grpcHost, grpcPort)
    val web = new WebServer(httpPort, new AdamController(grpc)).main(Array[String]())
  }
}
