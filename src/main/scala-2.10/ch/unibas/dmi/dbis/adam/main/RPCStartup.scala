package ch.unibas.dmi.dbis.adam.main

import ch.unibas.dmi.dbis.adam.rpc.RPCServer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class RPCStartup extends Thread {
  override def run() : Unit = {
    val server = new RPCServer(scala.concurrent.ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }
}
