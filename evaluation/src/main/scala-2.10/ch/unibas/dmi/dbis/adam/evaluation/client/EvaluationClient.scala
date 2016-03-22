package ch.unibas.dmi.dbis.adam.evaluation.client

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.evaluation.config.IndexTypes
import ch.unibas.dmi.dbis.adam.http.grpc.adam.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import ch.unibas.dmi.dbis.adam.http.grpc.adam.AdamSearchGrpc.{AdamSearchStub, AdamSearchBlockingStub}
import ch.unibas.dmi.dbis.adam.http.grpc.adam._
import io.grpc.ManagedChannel
import io.grpc.stub.StreamObserver
import org.apache.logging.log4j.LogManager

import scala.util.Random

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
class EvaluationClient(channel: ManagedChannel, definer: AdamDefinitionBlockingStub, searcherBlocking: AdamSearchBlockingStub, searcher: AdamSearchStub) {
  val log = LogManager.getLogger

  /**
    *
    * @param len
    * @return
    */
  private def getRandomName(len: Int = 10) = {
    val sb = new StringBuilder(len)
    val ab = "abcdefghijklmnopqrstuvwxyz"
    for (i <- 0 until len) {
      sb.append(ab(Random.nextInt(ab.length)))
    }
    sb.toString
  }

  /**
    *
    * @param entityname
    * @return
    */
  def createEntity(entityname: String = getRandomName()): String = {
    val ackMsg = definer.createEntity(CreateEntityMessage(entityname))

    if (ackMsg.code == AckMessage.Code.OK) {
      entityname
    } else {
      throw new Exception(ackMsg.message)
    }
  }

  /**
    *
    * @param entityname
    * @return
    */
  def dropEntity(entityname: String) = {
    definer.dropEntity(EntityNameMessage(entityname))
  }

  /**
    *
    * @param entityname
    * @return
    */
  def createIndex(entityname: String, indextype: IndexTypes.IndexType, norm: Int) = {
    definer.index(IndexMessage(entityname, indextype.indextype, norm))
  }

  /**
    *
    * @param entityname
    * @return
    */
  def sequentialQuery(entityname: String, q: Seq[Float], k: Int) = {
    val norm = 1
    searcherBlocking.doSequentialQuery(SimpleSequentialQueryMessage(entityname, Option(NearestNeighbourQueryMessage(q, norm, k, false))))
      .responses.map(resp => (resp.id, resp.distance))
  }

  /**
    *
    * @param entityname
    * @return
    */
  def indexQuery(entityname: String, indextype: IndexTypes.IndexType, q: Seq[Float], k: Int) = {
    val norm = 1
    searcherBlocking.doIndexQuery(SimpleIndexQueryMessage(entityname, indextype.indextype, Option(NearestNeighbourQueryMessage(q, norm, k, false))))
      .responses.map(resp => (resp.id, resp.distance))
  }

  /**
    *
    * @param entityname
    * @return
    */
  def progressiveQuery(entityname: String, q: Seq[Float], k: Int, completed : (Double, Seq[(Long, Float)], IndexTypes.IndexType) => Unit) = {
    val norm = 1

    val observer = new StreamObserver[QueryResponseInfoMessage] {
      def onNext(response: QueryResponseInfoMessage) {
        val confidence = response.confidence
        val results = response.queryResponseList.get.responses.map(resp => (resp.id, resp.distance))
        val indextype = response.indextype

        completed(confidence, results, IndexTypes.withIndextype(indextype).get)
      }

      def onError(t: Throwable): Unit = {
        log.error(t.getMessage)
      }

      def onCompleted(): Unit = {
        log.info("completed progressive query")
      }
    }

    searcher.doProgressiveQuery(SimpleQueryMessage(entityname, "", Option(NearestNeighbourQueryMessage(q, norm, k, false))), observer)
  }

  /**
    *
    * @param entityname
    * @param collectionSize
    * @param vectorSize
    */
  def generateRandomData(entityname : String, collectionSize : Int, vectorSize : Int): Unit = {
    definer.randomData(RandomDataMessage(entityname, collectionSize, vectorSize))
  }


  /**
    *
    */
  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }
}