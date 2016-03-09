package ch.unibas.dmi.dbis.adam.main

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.http.grpc.adam.{AdamSearchGrpc, AdamDefinitionGrpc}
import ch.unibas.dmi.dbis.adam.rpc.{SearchRPC, DataDefinitionRPC}
import io.grpc.Server
import io.grpc.netty.NettyServerBuilder

import scala.concurrent.ExecutionContext

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

class RPCServer(executionContext: ExecutionContext) { self =>
  private var server: Server = null

  private val port = AdamConfig.grpcPort

  def start(): Unit = {
    server = NettyServerBuilder.forPort(port).
      addService(AdamDefinitionGrpc.bindService(new DataDefinitionRPC, executionContext)).
      addService(AdamSearchGrpc.bindService(new SearchRPC, executionContext)).
      build.start
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        self.stop()
      }
    })
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  def blockUntilShutdown(): Unit = {
    while(true){
      Thread.sleep(1000)
    }
  }
}
