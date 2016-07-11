package ch.unibas.dmi.dbis.adam.rpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.http.grpc.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import ch.unibas.dmi.dbis.adam.http.grpc.AdamSearchGrpc.{AdamSearchBlockingStub, AdamSearchStub}
import ch.unibas.dmi.dbis.adam.http.grpc.DataMessage.Datatype
import ch.unibas.dmi.dbis.adam.http.grpc.DistanceMessage.DistanceType
import ch.unibas.dmi.dbis.adam.http.grpc.RepartitionMessage.PartitionOptions
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.rpc.datastructures.{RPCAttributeDefinition, RPCQueryObject, RPCQueryResults}
import ch.unibas.dmi.dbis.adam.utils.Logging
import io.grpc.okhttp.OkHttpChannelBuilder
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.util.{Failure, Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
//TODO: careful: duplicate code in client
class RPCClient(channel: ManagedChannel, definer: AdamDefinitionBlockingStub, searcherBlocking: AdamSearchBlockingStub, searcher: AdamSearchStub) extends Logging {
  /**
    *
    * @param desc description
    * @param op   operation
    * @return
    */
  private def execute[T](desc: String)(op: => Try[T]): Try[T] = {
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
    * Create an entity.
    *
    * @param entityname name of entity
    * @param attributes attributes of new entity
    * @return
    */
  def entityCreate(entityname: String, attributes: Seq[RPCAttributeDefinition]): Try[String] = {
    execute("create entity operation") {
      val attributeMessages = attributes.map { attribute =>
        val adm = AttributeDefinitionMessage(attribute.name, getFieldType(attribute.datatype), attribute.pk, unique = attribute.unique, indexed = attribute.indexed)

        //add handler information if available
        if (attribute.storagehandlername.isDefined) {
          val storagehandlername = attribute.storagehandlername.get

          val handlertype = storagehandlername match {
            case "relational" => HandlerType.relational
            case "feature" => HandlerType.feature
            case "solr" => HandlerType.solr
          }

          adm.withHandler(handlertype)
        }

        adm
      }

      val res = definer.createEntity(CreateEntityMessage(entityname, attributeMessages))
      if (res.code == AckMessage.Code.OK) {
        return Success(res.message)
      } else {
        return Failure(new Exception(res.message))
      }
    }
  }

  /**
    * Generate random data and fill into entity.
    *
    * @param entityname name of entity
    * @param tuples     number of tuples
    * @param dimensions dimensionality for feature fields
    * @param sparsity   sparsity of data for feature fields
    * @param min        min value for feature fields
    * @param max        max value for feature fields
    * @param sparse     is feature field sparse or dense
    * @return
    */
  def entityGenerateRandomData(entityname: String, tuples: Int, dimensions: Int, sparsity: Float, min: Float, max: Float, sparse: Boolean): Try[Void] = {
    execute("insert data operation") {

      val options = Map("fv-dimensions" -> dimensions, "fv-sparsity" -> sparsity, "fv-min" -> min, "fv-max" -> max, "fv-sparse" -> sparse).mapValues(_.toString)
      val res = definer.generateRandomData(GenerateRandomDataMessage(entityname, tuples, options))

      if (res.code == AckMessage.Code.OK) {
        return Success(null)
      } else {
        return Failure(new Exception(res.message))
      }
    }
  }


  /**
    * Import data to entity.
    *
    * @param host     host
    * @param database database
    * @param username username
    * @param password password
    * @return
    */
  def entityImport(host: String, database: String, username: String, password: String): Try[Void] = {
    execute("entity import operation") {
      definer.importData(ImportMessage(host, database, username, password))
      Success(null)
    }
  }


  /**
    * List all entities.
    *
    * @return
    */
  def entityList(): Try[Seq[String]] = {
    execute("list entities operation") {
      Success(definer.listEntities(EmptyMessage()).entities.sorted)
    }
  }


  /**
    * Get details for entity.
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
    * Partition entity.
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
    * Read data of entity.
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
    * Caches an entity.
    *
    * @param entityname
    */
  def entityCache(entityname : String): Try[Boolean] ={
    execute("cache entity") {
      val res = searcherBlocking.cacheEntity(EntityNameMessage(entityname))
      if (res.code.isOk) {
        Success(res.code.isOk)
      } else {
        throw new Exception("caching not possible: " + res.message)
      }
    }
  }

  /**
    * Benchmark entity and update scan weights.
    *
    * @param entityname name of entity
    * @param attribute  name of feature attribute
    * @return
    */
  def entityBenchmarkAndUpdateScanWeights(entityname: String, attribute: String): Try[Void] = {
    execute("benchmark entity scans and reset weights operation") {
      definer.benchmarkAndUpdateScanWeights(WeightMessage(entityname, attribute))
      Success(null)
    }
  }

  /**
    * Sparsify entity and store feature vectors as sparse vectors.
    *
    * @param entityname name of entity
    * @param attribute  name of feature attribute
    * @return
    */
  def entitySparsify(entityname: String, attribute: String): Try[Void] = {
    execute("benchmark entity scans and reset weights operation") {
      definer.sparsifyEntity(WeightMessage(entityname, attribute))
      Success(null)
    }
  }


  /**
    * Drop entity.
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
    * Create all indexes for entity.
    *
    * @param entityname name of entity
    * @param attributes name of attributes
    * @param norm       norm for distance function
    * @return
    */
  def entityCreateAllIndexes(entityname: String, attributes: Seq[String], norm: Int): Try[Seq[String]] = {
    execute("create all indexes operation") {
      val res = attributes.map { attribute => definer.generateAllIndexes(IndexMessage(entity = entityname, column = attribute, distance = Some(DistanceMessage(DistanceType.minkowski, options = Map("norm" -> norm.toString)))))
      }

      if (res.exists(_.code != AckMessage.Code.OK)) {
        val message = res.filter(_.code != AckMessage.Code.OK).map(_.message).mkString("; ")
        return Failure(new Exception(message))
      } else {
        Success(res.flatMap(_.message.split(",")))
      }
    }
  }


  /**
    * Create specific index.
    *
    * @param entityname name of entity
    * @param attribute  name of attribute
    * @param indextype  type of index
    * @param norm       norm
    * @param options    index creation options
    * @return
    */
  def indexCreate(entityname: String, attribute: String, indextype: String, norm: Int, options: Map[String, String]): Try[String] = {
    execute("create index operation") {
      val indexMessage = IndexMessage(entityname, attribute, getIndexType(indextype), Some(DistanceMessage(DistanceType.minkowski, Map("norm" -> norm.toString))), options)
      val res = definer.index(indexMessage)

      if (res.code == AckMessage.Code.OK) {
        return Success(res.message)
      } else {
        throw new Exception(res.message)
      }
    }
  }


  /**
    * Caches an index.
    *
    * @param indexname
    */
  def indexCache(indexname : String): Try[Boolean] ={
    execute("cache index") {
      val res = searcherBlocking.cacheIndex(IndexNameMessage(indexname))
      if (res.code.isOk) {
        Success(res.code.isOk)
      } else {
        throw new Exception("caching not possible: " + res.message)
      }
    }
  }

  /**
    *
    * @param s
    * @return
    */
  private def getIndexType(s : String) = s match {
    case "ecp" => IndexType.ecp
    case "lsh" => IndexType.lsh
    case "mi" => IndexType.mi
    case "pq" => IndexType.pq
    case "sh" => IndexType.sh
    case "vaf" => IndexType.vaf
    case "vav" => IndexType.vav
    case _ => null
  }


  /**
    * Partition index.
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

      if (res.code.isOk) {
        Success(res.message)
      } else {
        throw new Exception(res.message)
      }
    }
  }

  /**
    * Perform a search.
    *
    * @param qo search request
    * @return
    */
  def doQuery(qo: RPCQueryObject): Try[Seq[RPCQueryResults]] = {
    execute("compound query operation") {
      val res = searcherBlocking.doQuery(qo.getQueryMessage)
      if (res.ack.get.code.isOk) {
        return Success(res.responses.map(new RPCQueryResults(_)))
      } else {
        throw new Exception(res.ack.get.message)
      }
    }
  }

  /**
    * Perform a progressive search.
    *
    * @param qo search request
    * @param next       function for next result
    * @param completed  function for final result
    * @return
    */
  def doProgressiveQuery(qo: RPCQueryObject,  next: (Try[(String, Double, String, Long, Seq[Map[String, String]])]) => (Unit), completed: (String) => (Unit)): Try[Seq[RPCQueryResults]] = {
    execute("progressive query operation") {
      val so = new StreamObserver[QueryResultsMessage]() {
        override def onError(throwable: Throwable): Unit = {
          log.error("error in progressive querying", throwable)
        }

        override def onCompleted(): Unit = {
          completed(qo.id)
        }

        override def onNext(qr: QueryResultsMessage): Unit = {
          log.info("new progressive results arrived")

          if (qr.ack.get.code == AckMessage.Code.OK && qr.responses.nonEmpty) {
            val head = qr.responses.head

            val confidence = head.confidence
            val source = head.source
            val time = head.time
            val results = head.results.map(x => x.data.mapValues(_.toString()))

            next(Success(qo.id, confidence, source, time, results))
          } else {
            next(Failure(new Exception(qr.ack.get.message)))
          }
        }
      }

      searcher.doProgressiveQuery(qo.getQueryMessage, so)
      Success(null)
    }
  }

  /**
    * Shutdown connection.
    */
  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }



  /**
    *
    * @param s string of field type name
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