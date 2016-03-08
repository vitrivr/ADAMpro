package ch.unibas.dmi.dbis.adam.client

import ch.unibas.dmi.dbis.adam.http.grpc.adam.{AdamDefinitionGrpc, AdamSearchGrpc}
import io.grpc.ManagedChannelBuilder

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object ClientStartup {
  def main(args: Array[String]) {
    val client = ClientStartup("localhost", 5890)
    try {
      println(client.createEntity("test"))
    } finally {
      client.shutdown()
    }
  }

  def apply(host: String, port: Int): AdamClientImpl = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).asInstanceOf[ManagedChannelBuilder[_]].build()

    new AdamClientImpl(
      channel,
      AdamDefinitionGrpc.blockingStub(channel),
      AdamSearchGrpc.blockingStub(channel),
      AdamSearchGrpc.stub(channel)
    )
  }
}
