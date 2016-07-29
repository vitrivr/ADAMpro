package ch.unibas.dmi.dbis.adam.web.main

import ch.unibas.dmi.dbis.adam.web.AdamController
import ch.unibas.dmi.dbis.adam.web.WebServer
import ch.unibas.dmi.dbis.adam.rpc.RPCClient

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object ClientStartup {
  val httpPort = 9099

  val grpcHost = "10.34.58.136"
  val grpcPort = 5890

  def main(args: Array[String]): Unit = {
    val grpc = RPCClient(grpcHost, grpcPort)

    val web = new WebServer(httpPort, new AdamController(grpc)).main(Array[String]())
  }
}
