package ch.unibas.dmi.dbis.adam.client.grpc

import java.util.concurrent.TimeUnit
import ch.unibas.dmi.dbis.adam.client.web.datastructures.{CompoundQueryResponse, CompoundQueryRequest}
import ch.unibas.dmi.dbis.adam.http.grpc.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import ch.unibas.dmi.dbis.adam.http.grpc.AdamSearchGrpc.{AdamSearchBlockingStub, AdamSearchStub}
import ch.unibas.dmi.dbis.adam.http.grpc.DistanceMessage.DistanceType
import ch.unibas.dmi.dbis.adam.http.grpc._
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
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

  def addIndex(entityname: String, indextype: IndexType, norm: Int, options: Map[String, String]): String = {
    val indexMessage = IndexMessage(entityname, indextype, norm, options)
    val res = definer.index(indexMessage)
    if (res.code == AckMessage.Code.OK) {
      res.message
    } else {
      log.error(res.message)
      ""
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
  def progressiveQuery(id : String, entityname: String, query: Seq[Float], hints : Seq[String], k: Int, next: (String, Double, String, Long, Seq[(Float, Long)]) => (Unit), completed: (String) => (Unit)) : String = {
    val nnq = NearestNeighbourQueryMessage(query, Option(DistanceMessage(DistanceType.minkowski, Map("norm" -> "2"))), k)
    val request = SimpleQueryMessage(entity = entityname, hints = hints, nnq = Option(nnq))

    val so = new StreamObserver[QueryResponseInfoMessage]() {
      override def onError(throwable: Throwable): Unit = {
        log.error(throwable)
      }

      override def onCompleted(): Unit = {
        completed(id)
      }

      override def onNext(v: QueryResponseInfoMessage): Unit = {
        val confidence = v.confidence
        val source = v.source
        val time = v.time
        val results = v.results.map(x => (x.distance, x.id))

        next(id, confidence, source, time, results)
      }
    }

    searcher.doProgressiveQuery(request, so)

    id
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