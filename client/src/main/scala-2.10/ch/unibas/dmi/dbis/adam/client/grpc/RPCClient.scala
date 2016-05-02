package ch.unibas.dmi.dbis.adam.client.grpc

import java.util.concurrent.TimeUnit
import ch.unibas.dmi.dbis.adam.client.web.datastructures.{EntityField, CompoundQueryResponse, CompoundQueryRequest}
import ch.unibas.dmi.dbis.adam.http.grpc.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import ch.unibas.dmi.dbis.adam.http.grpc.AdamSearchGrpc.{AdamSearchBlockingStub, AdamSearchStub}
import ch.unibas.dmi.dbis.adam.http.grpc.DistanceMessage.DistanceType
import ch.unibas.dmi.dbis.adam.http.grpc.FieldDefinitionMessage.FieldType
import ch.unibas.dmi.dbis.adam.http.grpc.RepartitionMessage.PartitionOptions
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
    * @param fields
    * @return
    */
  def createEntity(entityname: String, fields: Seq[EntityField]): Boolean = {
    log.info("creating entity")
    val fieldMessage = fields.map(field =>
      FieldDefinitionMessage(field.name, getFieldType(field.datatype), false, false, field.indexed)
    )

    val res = definer.createEntity(CreateEntityMessage(entityname, fieldMessage))
    log.info("created entity : " + res.code.toString() + " " + res.message)
    if (res.code == AckMessage.Code.OK) {
      return true
    } else {
      return false
    }

  }


  /**
    *
    * @param entityname
    * @param ntuples
    * @param ndims
    * @param fields
    * @return
    */
  def prepareDemo(entityname: String, ntuples: Int, ndims: Int, fields: Seq[EntityField]): Boolean = {
    log.info("preparing demo data")
    val fieldMessage = fields.map(field =>
      FieldDefinitionMessage(field.name, getFieldType(field.datatype), false, false, field.indexed)
    )

    val res = definer.generateRandomData(GenerateRandomDataMessage(entityname, ntuples, ndims))
    log.info("prepared demo data: " + res.code.toString() + " " + res.message)
    if (res.code == AckMessage.Code.OK) {
      return true
    } else {
      return false
    }
  }

  /**
    *
    * @param s
    * @return
    */
  private def getFieldType(s: String): FieldDefinitionMessage.FieldType = s match {
    case "long" => FieldType.LONG
    case "int" => FieldType.INT
    case "float" => FieldType.FLOAT
    case "double" => FieldType.DOUBLE
    case "string" => FieldType.STRING
    case "boolean" => FieldType.BOOLEAN
    case _ => null
  }

  /**
    *
    * @return
    */
  def listEntities(): Seq[String] = {
    definer.listEntities(Empty()).entities
  }

  /**
    *
    * @return
    */
  def getDetails(entityname: String): Map[String, String] = {
    definer.getEntityProperties(EntityNameMessage(entityname)).properties
  }

  /**
    *
    * @param entityname
    * @param indextype
    * @param norm
    * @param options
    * @return
    */
  def addIndex(entityname: String, indextype: IndexType, norm: Int, options: Map[String, String]): String = {
    val indexMessage = IndexMessage(entityname, indextype, Some(DistanceMessage(DistanceType.minkowski, Map("norm" -> norm.toString))), options)
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
    * @param entityname
    * @return
    */
  def addAllIndex(entityname: String, norm: Int): Boolean = {
    val res = definer.generateAllIndexes(IndexMessage(entity = entityname, distance = Some(DistanceMessage(DistanceType.minkowski, options = Map("norm" -> norm.toString)))))
    if (res.code == AckMessage.Code.OK) {
      true
    } else {
      log.error(res.message)
      false
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
  def progressiveQuery(id: String, entityname: String, query: Seq[Float], hints: Seq[String], k: Int, next: (String, Double, String, Long, Seq[(Float, Long)]) => (Unit), completed: (String) => (Unit)): String = {
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
    * @param index
    * @param partitions
    * @return
    */
  def repartition(index: String, partitions: Int, useMetadata: Boolean = false, cols: Seq[String] = Seq(), materialize: Boolean, replace: Boolean): String = {
    val option = if (replace) {
      PartitionOptions.REPLACE_EXISTING
    } else if (materialize) {
      PartitionOptions.CREATE_NEW
    } else if (!materialize) {
      PartitionOptions.CREATE_TEMP
    } else {
      PartitionOptions.CREATE_NEW
    }

    definer.repartitionIndexData(RepartitionMessage(index, partitions, useMetadata, cols, option)).message
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