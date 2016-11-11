package org.vitrivr.adampro.web.main

import org.vitrivr.adampro.web.controller.{WebServer, AdamController}
import org.vitrivr.adampro.rpc.RPCClient

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object ClientStartup {
  val httpPort = 9099

  val grpcHost = "localhost"
  val grpcPort = 5890

  def main(args: Array[String]): Unit = {
    val grpc = RPCClient(grpcHost, grpcPort)

    val web = new WebServer(httpPort, new AdamController(grpc)).main(Array[String]())
  }
}
