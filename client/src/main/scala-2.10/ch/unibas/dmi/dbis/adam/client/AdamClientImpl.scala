package ch.unibas.dmi.dbis.adam.client

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.http.grpc.adam.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import ch.unibas.dmi.dbis.adam.http.grpc.adam.AdamSearchGrpc.{AdamSearchStub, AdamSearchBlockingStub}
import ch.unibas.dmi.dbis.adam.http.grpc.adam.EntityNameMessage
import io.grpc.ManagedChannel

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class AdamClientImpl(channel: ManagedChannel, definer: AdamDefinitionBlockingStub, searcherBlocking : AdamSearchBlockingStub, searcher : AdamSearchStub ) {
  def createEntity(entityname : String) = definer.createEntity(EntityNameMessage(entityname))

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }


}