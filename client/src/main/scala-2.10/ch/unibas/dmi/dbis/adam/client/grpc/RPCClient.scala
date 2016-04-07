package ch.unibas.dmi.dbis.adam.client.grpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.client.web.CompoundQueryRequest
import ch.unibas.dmi.dbis.adam.http.grpc.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import ch.unibas.dmi.dbis.adam.http.grpc.AdamSearchGrpc.{AdamSearchBlockingStub, AdamSearchStub}
import ch.unibas.dmi.dbis.adam.http.grpc.{AdamDefinitionGrpc, AdamSearchGrpc, GenerateRandomEntityMessage}
import io.grpc.{ManagedChannelBuilder, ManagedChannel}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class RPCClient(channel: ManagedChannel, definer: AdamDefinitionBlockingStub, searcherBlocking : AdamSearchBlockingStub, searcher : AdamSearchStub ) {

  def prepareDemo(entityname : String, ntuples : Int, ndims : Int) = {
    definer.prepareForDemo(GenerateRandomEntityMessage(entityname, ntuples, ndims))
  }

  def compoundQuery(request : CompoundQueryRequest): Unit ={
    searcherBlocking.doCompoundQuery(request.toRPCMessage())
  }

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }
}


object RPCClient {
  def apply(host: String, port: Int): RPCClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).asInstanceOf[ManagedChannelBuilder[_]].build()

    new RPCClient(
      channel,
      AdamDefinitionGrpc.blockingStub(channel),
      AdamSearchGrpc.blockingStub(channel),
      AdamSearchGrpc.stub(channel)
    )
  }
}