package ch.unibas.dmi.dbis.adam.client.grpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.client.web.{CompoundQueryResponse, CompoundQueryRequest}
import ch.unibas.dmi.dbis.adam.http.grpc.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import ch.unibas.dmi.dbis.adam.http.grpc.AdamSearchGrpc.{AdamSearchBlockingStub, AdamSearchStub}
import ch.unibas.dmi.dbis.adam.http.grpc._
import io.grpc.{ManagedChannelBuilder, ManagedChannel}
import org.apache.log4j.Logger

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class RPCClient(channel: ManagedChannel, definer: AdamDefinitionBlockingStub, searcherBlocking: AdamSearchBlockingStub, searcher: AdamSearchStub) {
  val log = Logger.getLogger(getClass.getName)

  /**
    *
    * @param entityname
    * @param ntuples
    * @param ndims
    * @return
    */
  def prepareDemo(entityname: String, ntuples: Int, ndims: Int): Boolean = {
    log.info("preparing demo data")
    val res = definer.prepareForDemo(GenerateRandomEntityMessage(entityname, ntuples, ndims))
    log.info("prepared demo data: " + res.code.toString())
    if (res.code == AckMessage.Code.OK) {
      return true
    } else {
      return false
    }
  }

  /**
    *
    * @param request
    * @return
    */
  def compoundQuery(request: CompoundQueryRequest): CompoundQueryResponse = {
    log.info("compound query start")
    val res = searcherBlocking.doCompoundQuery(request.toRPCMessage())
    log.info("compound query results received")
    new CompoundQueryResponse(res)
  }

  /**
    *
    */
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