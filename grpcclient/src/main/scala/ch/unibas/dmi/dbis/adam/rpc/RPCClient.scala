package ch.unibas.dmi.dbis.adam.rpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.rpc.datastructures.{RPCAttributeDefinition, RPCQueryObject, RPCQueryResults}
import ch.unibas.dmi.dbis.adam.utils.Logging
import io.grpc.internal.DnsNameResolverProvider
import io.grpc.okhttp.OkHttpChannelBuilder
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.vitrivr.adam.grpc.grpc.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import org.vitrivr.adam.grpc.grpc.AdamSearchGrpc.{AdamSearchStub, AdamSearchBlockingStub}
import org.vitrivr.adam.grpc.grpc.AdaptScanMethodsMessage.IndexCollection.NEW_INDEXES
import org.vitrivr.adam.grpc.grpc.AdaptScanMethodsMessage.QueryCollection.RANDOM_QUERIES
import org.vitrivr.adam.grpc.grpc.DistanceMessage.DistanceType
import org.vitrivr.adam.grpc.grpc.RepartitionMessage.PartitionOptions
import org.vitrivr.adam.grpc.grpc._

import scala.util.{Failure, Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
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
      val res = op
      val t2 = System.currentTimeMillis
      log.debug("performed " + desc + " in " + (t2 - t1) + " msecs")
      res
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
        val adm = AttributeDefinitionMessage(attribute.name, getAttributeType(attribute.datatype), attribute.pk, params = attribute.params)

        //add handler information if available
        if (attribute.storagehandlername.isDefined) {
          val storagehandlername = attribute.storagehandlername.get

          val handlertype = storagehandlername match {
            case "relational" => HandlerType.RELATIONAL
            case "file" => HandlerType.FILE
            case "solr" => HandlerType.SOLR
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
    * Check if entity exists.
    *
    * @param entityname name of entity
    * @return
    */
  def entityExists(entityname: String): Try[Boolean] = {
    execute("entity exists operation") {
      val res = definer.existsEntity(EntityNameMessage(entityname))
      if (res.ack.get.code == AckMessage.Code.OK) {
        return Success(res.exists)
      } else {
        return Failure(new Exception(res.ack.get.message))
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
      var properties = definer.getEntityProperties(EntityNameMessage(entityname)).properties

      if(count.code == AckMessage.Code.OK){
        properties = properties.+("count" -> count.message)
      }

      Success(properties)
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
  def entityRead(entityname: String): Try[Seq[RPCQueryResults]] = {
    execute("get entity data operation") {
      val res = searcherBlocking.preview(PreviewMessage(entityname))
      Success(res.responses.map(new RPCQueryResults(_)))
    }
  }


  /**
    * Caches an entity.
    *
    * @param entityname
    */
  def entityCache(entityname: String): Try[Boolean] = {
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
      definer.adaptScanMethods(AdaptScanMethodsMessage(entityname, attribute, NEW_INDEXES, RANDOM_QUERIES))
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
      definer.sparsifyEntity(SparsifyEntityMessage(entityname, attribute))
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
      val res = attributes.map { attribute => definer.generateAllIndexes(IndexMessage(entity = entityname, attribute = attribute, distance = Some(DistanceMessage(DistanceType.minkowski, options = Map("norm" -> norm.toString)))))
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
    * List all indexes for given entity.
    *
    * @param entityname name of entity
    * @return (indexname, attribute, indextypename)
    */
  def indexList(entityname: String): Try[Seq[(String, String, IndexType)]] = {
    execute("list indexes operation") {
      Success(definer.listIndexes(EntityNameMessage(entityname)).indexes.map(i => (i.index, i.attribute, i.indextype)))
    }
  }

  /**
    * Check if index exists.
    *
    * @param entityname name of entity
    * @param attribute  nmae of attribute
    * @param indextype  type of index
    * @return
    */
  def indexExists(entityname: String, attribute: String, indextype: String): Try[Boolean] = {
    execute("index exists operation") {
      val res = definer.existsIndex(IndexMessage(entityname, attribute, getIndexType(indextype)))
      if (res.ack.get.code == AckMessage.Code.OK) {
        return Success(res.exists)
      } else {
        return Failure(new Exception(res.ack.get.message))
      }
    }
  }


  /**
    * Caches an index.
    *
    * @param indexname
    */
  def indexCache(indexname: String): Try[Boolean] = {
    execute("cache index operation") {
      val res = searcherBlocking.cacheIndex(IndexNameMessage(indexname))
      if (res.code.isOk) {
        Success(res.code.isOk)
      } else {
        throw new Exception("caching not possible: " + res.message)
      }
    }
  }

  /**
    * Drop index.
    *
    * @param indexname name of index
    */
  def indexDrop(indexname: String): Try[Void] = {
    execute("drop index operation") {
      definer.dropIndex(IndexNameMessage(indexname))
      Success(null)
    }
  }

  /**
    *
    * @param s
    * @return
    */
  private def getIndexType(s: String) = s match {
    case "ecp" => IndexType.ecp
    case "lsh" => IndexType.lsh
    case "mi" => IndexType.mi
    case "pq" => IndexType.pq
    case "sh" => IndexType.sh
    case "vaf" => IndexType.vaf
    case "vav" => IndexType.vav
    case "vap" => IndexType.vap
    case _ => throw new Exception("no indextype of name " + s + " known")
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
    * @param qo        search request
    * @param next      function for next result
    * @param completed function for final result
    * @return
    */
  def doProgressiveQuery(qo: RPCQueryObject, next: (Try[RPCQueryResults]) => (Unit), completed: (String) => (Unit)): Try[Seq[RPCQueryResults]] = {
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
            next(Success(new RPCQueryResults(qr.responses.head)))
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
    * Returns registered storage handlers.
    *
    * @return
    */
  def storageHandlerList(): Try[Map[String, Seq[String]]] = {
    execute("get storage handlers operation") {
      Success(definer.listStorageHandlers(EmptyMessage()).handlers.map(handler => handler.name -> handler.attributetypes.map(_.toString)).toMap)
    }
  }

  /**
    * Shutdown connection.
    */
  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }


  val fieldtypemapping = Map("feature" -> AttributeType.FEATURE, "long" -> AttributeType.LONG, "int" -> AttributeType.INT, "float" -> AttributeType.FLOAT,
    "double" -> AttributeType.DOUBLE, "string" -> AttributeType.STRING, "text" -> AttributeType.TEXT, "boolean" -> AttributeType.BOOLEAN, "geography" -> AttributeType.GEOGRAPHY,
    "geometry" -> AttributeType.GEOMETRY)

  val attributetypemapping = fieldtypemapping.map(_.swap)

  /**
    *
    * @param s string of field type name
    * @return
    */
  private def getAttributeType(s: String): AttributeType = fieldtypemapping.get(s).orNull

  private def getFieldTypeName(a: AttributeType): String = attributetypemapping.get(a).orNull

  //TODO: add get attributes-method for an entity, to retrieve attributes to display
}

object RPCClient {
  def apply(host: String, port: Int): RPCClient = {
    val channel = OkHttpChannelBuilder.forAddress(host, port)
      .nameResolverFactory(new DnsNameResolverProvider())
      .usePlaintext(true)
      .asInstanceOf[ManagedChannelBuilder[_]].build()

    new RPCClient(
      channel,
      AdamDefinitionGrpc.blockingStub(channel),
      AdamSearchGrpc.blockingStub(channel),
      AdamSearchGrpc.stub(channel)
    )
  }
}