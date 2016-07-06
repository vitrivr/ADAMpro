package ch.unibas.dmi.dbis.adam.client.grpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.client.web.datastructures.{SearchResponse, SearchCompoundRequest, EntityField}
import ch.unibas.dmi.dbis.adam.http.grpc.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import ch.unibas.dmi.dbis.adam.http.grpc.AdamSearchGrpc.{AdamSearchBlockingStub, AdamSearchStub}
import ch.unibas.dmi.dbis.adam.http.grpc.DataMessage.Datatype
import ch.unibas.dmi.dbis.adam.http.grpc.DistanceMessage.DistanceType
import ch.unibas.dmi.dbis.adam.http.grpc.RepartitionMessage.PartitionOptions
import ch.unibas.dmi.dbis.adam.http.grpc._
import io.grpc.okhttp.OkHttpChannelBuilder
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

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
    * @param desc
    * @param op
    * @tparam T
    * @return
    */
  def execute[T](desc: String)(op: => Try[T]): Try[T] = {
    try {
      log.debug("starting " + desc)
      val t1 = System.currentTimeMillis
      val result = op
      val t2 = System.currentTimeMillis
      log.debug("performed " + desc + " in " + (t2 - t1) + " msecs")
      result
    } catch {
      case e: Exception =>
        log.error("error in " + desc, e)
        Failure(e)
    }
  }

  /**
    *
    * @param entityname name of entity
    * @param attributes attributes of new entity
    * @return
    */
  def entityCreate(entityname: String, attributes: Seq[EntityField]): Try[String] = {
    execute("create entity operation") {
      val fieldMessage = attributes.map(field =>
        AttributeDefinitionMessage(field.name, getFieldType(field.datatype), field.pk, false, field.indexed)
      )

      val res = definer.createEntity(CreateEntityMessage(entityname, fieldMessage))
      if (res.code == AckMessage.Code.OK) {
        return Success(res.message)
      } else {
        return Failure(new Exception(res.message))
      }
    }
  }

  /**
    *
    * @param entityname name of entity
    * @param ntuples    number of tuples
    * @param ndims      dimensionality for feature fields
    * @return
    */
  def entityFill(entityname: String, ntuples: Int, ndims: Int): Try[Void] = {
    execute("insert data operation") {
      val res = definer.generateRandomData(GenerateRandomDataMessage(entityname, ntuples, ndims))

      if (res.code == AckMessage.Code.OK) {
        return Success(null)
      } else {
        return Failure(new Exception(res.message))
      }
    }
  }


  /**
    *
    * @param host     host
    * @param database database
    * @param username username
    * @param password password
    * @return
    */
  def entityImport(host: String, database: String, username: String, password: String): Try[Void] = {
    try {
      definer.importData(ImportMessage(host, database, username, password))
      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    *
    * @return
    */
  def entityList(): Try[Seq[String]] = {
    execute("list entities operation") {
      Success(definer.listEntities(EmptyMessage()).entities.sorted)
    }
  }


  /**
    *
    * @param entityname name of entity
    * @return
    */
  def entityDetails(entityname: String): Try[Map[String, String]] = {
    execute("get details of entity operation") {
      val count = definer.count(EntityNameMessage(entityname))
      val properties = definer.getEntityProperties(EntityNameMessage(entityname)).properties
      Success(properties.+("count" -> definer.count(EntityNameMessage(entityname)).message))
    }
  }


  /**
    *
    * @param entityname  name of entity
    * @param npartitions number of partitions
    * @param attributes  attributes
    * @param materialize materialize partitioning
    * @param replace     replace partitioning
    * @return
    */
  def entityPartition(entityname: String, npartitions: Int, attributes: Seq[String] = Seq(), materialize: Boolean, replace: Boolean): Try[String] = {
    execute("repartition entity operation") {
      val option = if (replace) {
        PartitionOptions.REPLACE_EXISTING
      } else if (materialize) {
        PartitionOptions.CREATE_NEW
      } else if (!materialize) {
        PartitionOptions.CREATE_TEMP
      } else {
        PartitionOptions.CREATE_NEW
      }

      val res = definer.repartitionEntityData(RepartitionMessage(entityname, npartitions, attributes, option))

      if (res.code == AckMessage.Code.OK) {
        Success(res.message)
      } else {
        Failure(throw new Exception(res.message))
      }
    }
  }


  /**
    *
    * @param entityname name of entity
    */
  def entityRead(entityname: String): Try[Seq[Map[String, String]]] = {
    execute("get entity data operation") {
      val res = searcherBlocking.preview(EntityNameMessage(entityname))

      val readable = res.responses.head.results.map(tuple => {
        tuple.data.map(attribute => {
          val key = attribute._1
          val value = attribute._2.datatype match {
            case Datatype.IntData(x) => x.toInt.toString
            case Datatype.LongData(x) => x.toLong.toString
            case Datatype.FloatData(x) => x.toFloat.toString
            case Datatype.DoubleData(x) => x.toDouble.toString
            case Datatype.StringData(x) => x.toString
            case Datatype.BooleanData(x) => x.toString
            case Datatype.FeatureData(x) => x.feature.denseVector.get.vector.mkString("[", ",", "]")
            case _ => ""
          }
          key -> value
        })
      })

      Success(readable)
    }
  }


  /**
    *
    * @param entityname name of entity
    * @param attribute  name of feature attribute
    * @return
    */
  def entityBenchmark(entityname: String, attribute: String): Try[Void] = {
    execute("benchmark entity scans and reset weights operation") {
      definer.benchmarkAndUpdateScanWeights(WeightMessage(entityname, attribute))
      Success(null)
    }
  }

  /**
    *
    * @param entityname name of entity
    * @param attribute  name of feature attribute
    * @return
    */
  def entitySparsify(entityname: String, attribute : String): Try[Void] = {
    execute("benchmark entity scans and reset weights operation") {
      definer.sparsifyEntity(WeightMessage(entityname, attribute))
      Success(null)
    }
  }


  /**
    *
    * @param entityname name of entity
    */
  def entityDrop(entityname: String): Try[Void] = {
    execute("drop entity operation") {
      definer.dropEntity(EntityNameMessage(entityname))
      Success(null)
    }
  }


  /**
    *
    * @param entityname name of entity
    * @param attributes name of attributes
    * @param norm       norm for distance function
    * @return
    */
  def entityCreateAll(entityname: String, attributes: Seq[EntityField], norm: Int): Try[Void] = {
    execute("create all indexes operation") {
      val fieldMessage = attributes.map(field =>
        AttributeDefinitionMessage(field.name, getFieldType(field.datatype), false, false, field.indexed)
      ).filter(_.attributetype == AttributeType.FEATURE)

      fieldMessage.map { column =>
        val res = definer.generateAllIndexes(IndexMessage(entity = entityname, column = column.name, distance = Some(DistanceMessage(DistanceType.minkowski, options = Map("norm" -> norm.toString)))))
        if (res.code != AckMessage.Code.OK) {
          return Failure(new Exception(res.message))
        }
      }

      Success(null)
    }
  }


  /**
    *
    * @param entityname name of entity
    * @param attribute  name of attribute
    * @param indextype  type of index
    * @param norm       norm
    * @param options    index creation options
    * @return
    */
  def indexCreate(entityname: String, attribute: String, indextype: IndexType, norm: Int, options: Map[String, String]): Try[String] = {
    execute("create index operation") {
      val indexMessage = IndexMessage(entityname, attribute, indextype, Some(DistanceMessage(DistanceType.minkowski, Map("norm" -> norm.toString))), options)
      val res = definer.index(indexMessage)

      if (res.code == AckMessage.Code.OK) {
        return Success(res.message)
      } else {
        return Failure(new Exception(res.message))
      }
    }
  }


  /**
    *
    * @param indexname   name of index
    * @param npartitions number of partitions
    * @param attributes  attributes
    * @param materialize materialize partitioning
    * @param replace     replace partitioning
    * @return
    */
  def indexPartition(indexname: String, npartitions: Int, attributes: Seq[String] = Seq(), materialize: Boolean, replace: Boolean): Try[String] = {
    execute("partition index operation") {
      val option = if (replace) {
        PartitionOptions.REPLACE_EXISTING
      } else if (materialize) {
        PartitionOptions.CREATE_NEW
      } else if (!materialize) {
        PartitionOptions.CREATE_TEMP
      } else {
        PartitionOptions.CREATE_NEW
      }

      val res = definer.repartitionIndexData(RepartitionMessage(indexname, npartitions, attributes, option))

      if (res.code == AckMessage.Code.OK) {
        Success(res.message)
      } else {
        Failure(throw new Exception(res.message))
      }
    }
  }


  /**
    *
    * @param s
    * @return
    */
  private def getFieldType(s: String): AttributeType = s match {
    case "feature" => AttributeType.FEATURE
    case "long" => AttributeType.LONG
    case "int" => AttributeType.INT
    case "float" => AttributeType.FLOAT
    case "double" => AttributeType.DOUBLE
    case "string" => AttributeType.STRING
    case "text" => AttributeType.TEXT
    case "boolean" => AttributeType.BOOLEAN
    case _ => null
  }


  /**
    *
    * @param request search compound request
    * @return
    */
  def searchCompound(request: SearchCompoundRequest): Try[SearchResponse] = {
    execute("compound query operation") {
      val res = searcherBlocking.doQuery(request.toRPCMessage())
      if (res.ack.get.code == AckMessage.Code.OK) {
        return Success(new SearchResponse(res))
      } else {
        return Failure(new Exception(res.ack.get.message))
      }
    }
  }

  /**
    *
    * @param queryid    query id
    * @param entityname name of entity
    * @param q          vector
    * @param attribute
    * @param hints      query hints
    * @param k          k of kNN
    * @param next       function for next result
    * @param completed  function for final result
    * @return
    */
  def searchProgressive(queryid: String, entityname: String, q: Seq[Float], attribute: String, hints: Seq[String], k: Int, next: (Try[(String, Double, String, Long, Seq[Map[String, String]])]) => (Unit), completed: (String) => (Unit)): Try[Void] = {
    execute("progressive query operation") {
      val fv = FeatureVectorMessage().withDenseVector(DenseVectorMessage(q))
      val nnq = NearestNeighbourQueryMessage(attribute, Some(fv), None, Option(DistanceMessage(DistanceType.minkowski, Map("norm" -> "2"))), k, indexOnly = true)
      val request = QueryMessage(from = Some(FromMessage().withEntity(entityname)), hints = hints, nnq = Option(nnq))

      val so = new StreamObserver[QueryResultsMessage]() {
        override def onError(throwable: Throwable): Unit = {
          log.error(throwable)
        }

        override def onCompleted(): Unit = {
          completed(queryid)
        }

        override def onNext(qr: QueryResultsMessage): Unit = {
          log.info("new progressive results arrived")

          if (qr.ack.get.code == AckMessage.Code.OK && !qr.responses.isEmpty) {
            val head = qr.responses.head

            val confidence = head.confidence
            val source = head.source
            val time = head.time
            val results = head.results.map(x => x.data.mapValues(x => ""))

            next(Success(queryid, confidence, source, time, results))
          } else {
            next(Failure(new Exception(qr.ack.get.message)))
          }
        }
      }

      searcher.doProgressiveQuery(request, so)
      Success(null)
    }
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
    val channel = OkHttpChannelBuilder.forAddress(host, port).usePlaintext(true).asInstanceOf[ManagedChannelBuilder[_]].build()

    new RPCClient(
      channel,
      AdamDefinitionGrpc.blockingStub(channel),
      AdamSearchGrpc.blockingStub(channel),
      AdamSearchGrpc.stub(channel)
    )
  }
}