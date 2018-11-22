package org.vitrivr.adampro.communication

import java.util.concurrent.TimeUnit

import com.trueaccord.scalapb.json.JsonFormat
import io.grpc.internal.DnsNameResolverProvider
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.vitrivr.adampro.communication.datastructures._
import org.vitrivr.adampro.grpc.grpc.AdamDefinitionGrpc.{AdamDefinitionBlockingStub, AdamDefinitionStub}
import org.vitrivr.adampro.grpc.grpc.AdamSearchGrpc.{AdamSearchBlockingStub, AdamSearchStub}
import org.vitrivr.adampro.grpc.grpc.AdaptScanMethodsMessage.IndexCollection.{EXISTING_INDEXES, NEW_INDEXES}
import org.vitrivr.adampro.grpc.grpc.AdaptScanMethodsMessage.QueryCollection.{LOGGED_QUERIES, RANDOM_QUERIES}
import org.vitrivr.adampro.grpc.grpc.DistanceMessage.DistanceType
import org.vitrivr.adampro.grpc.grpc.RepartitionMessage.PartitionOptions
import org.vitrivr.adampro.grpc.grpc.{AttributeType, _}
import org.vitrivr.adampro.utils.Logging

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, TimeoutException}
import scala.util.{Failure, Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class RPCClient(channel: ManagedChannel,
                private[adampro] val definerBlocking: AdamDefinitionBlockingStub,
                private[adampro] val definer: AdamDefinitionStub,
                private[adampro] val searcherBlocking: AdamSearchBlockingStub,
                private[adampro] val searcher: AdamSearchStub) extends Logging {
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
        var adm = AttributeDefinitionMessage(attribute.name, getGrpcType(attribute.datatype), params = attribute.params)

        //add handler information if available
        if (attribute.storagehandlername.isDefined) {
          adm = adm.withHandler(attribute.storagehandlername.get)
        }

        adm
      }

      val res = definerBlocking.createEntity(CreateEntityMessage(entityname, attributeMessages))
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
      val res = definerBlocking.existsEntity(EntityNameMessage(entityname))
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
    * @param entityname   name of entity
    * @param tuples       number of tuples
    * @param dimensions   dimensionality for feature fields
    * @param sparsity     sparsity of data for feature fields
    * @param min          min value for feature fields
    * @param max          max value for feature fields
    * @param distribution distribution for random data
    * @return
    */
  def entityGenerateRandomData(entityname: String, tuples: Int, dimensions: Int, sparsity: Float, min: Float, max: Float, distribution: Option[String]): Try[Void] = {
    execute("entity generate random data operation") {

      var options: Map[String, String] = Map("fv-dimensions" -> dimensions, "fv-sparsity" -> sparsity, "fv-min" -> min, "fv-max" -> max).mapValues(_.toString)

      if (distribution.isDefined) {
        options += "fv-distribution" -> distribution.get
      }

      val res = definerBlocking.generateRandomData(GenerateRandomDataMessage(entityname, tuples, options))

      if (res.code == AckMessage.Code.OK) {
        return Success(null)
      } else {
        return Failure(new Exception(res.message))
      }
    }
  }

  /**
    * Insert data into entity.
    *
    * @param insertMessage insert message
    * @return
    */
  def entityInsert(insertMessage: InsertMessage): Try[Void] = {
    execute("insert operation") {
      val res = definerBlocking.insert(insertMessage)

      if (res.code == AckMessage.Code.OK) {
        return Success(null)
      } else {
        return Failure(new Exception(res.message))
      }
    }
  }

  /**
    * Insert data into entity (streaming).
    *
    * @param insertMessages sequence of insert messages
    * @return
    */
  def entityStreamInsert(insertMessages: Seq[InsertMessage]): Try[Void] = {
    val so = new StreamObserver[AckMessage]() {
      override def onError(throwable: Throwable): Unit = {
        log.error("error in insert", throwable)
      }

      override def onCompleted(): Unit = {
        log.info("completed insert")
      }

      override def onNext(ack: AckMessage): Unit = {
        if (ack.code == AckMessage.Code.OK) {
          //no output on success
        } else {
          log.error("error in insert: " + ack.message)
        }
      }
    }

    val insertSo = definer.streamInsert(so)

    insertMessages.foreach(im => insertSo.onNext(_))

    Success(null)
  }

  /**
    * Import data to entity.
    *
    * @param path path
    * @param out  stream observer
    * @return
    */
  def entityProtoImport(path: String, out: StreamObserver[(Boolean, String)]): Try[Void] = {
    execute("entity import operation") {
      val so = new StreamObserver[AckMessage]() {
        override def onError(throwable: Throwable): Unit = out.onError(throwable)

        override def onCompleted(): Unit = out.onCompleted()

        override def onNext(ack: AckMessage): Unit = out.onNext((ack.code == AckMessage.Code.OK, ack.message))
      }

      definer.protoImportData(ProtoImportMessage(path), so)
      Success(null)
    }
  }

  /**
    * Export data from entity.
    *
    * @param path path
    * @param entity
    * @return
    */
  def entityProtoExport(path: String, entity: String): Try[Void] = {
    execute("entity import operation") {
      val res = definerBlocking.protoExportData(ProtoExportMessage(path, entity))

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
      definerBlocking.importData(ImportMessage(host, database, username, password))
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
      Success(definerBlocking.listEntities(EmptyMessage()).entities.sorted)
    }
  }


  /**
    * Get details for entity.
    *
    * @param entityname name of entity
    * @param options    options for operation
    * @return
    */
  def entityDetails(entityname: String, options: Map[String, String] = Map()): Try[Map[String, String]] = {
    execute("get details of entity operation") {
      val properties = definerBlocking.getEntityProperties(EntityPropertiesMessage(entityname, options)).properties
      Success(properties)
    }
  }

  /**
    * Get details for attribute.
    *
    * @param entityname name of entity
    * @param attribute  name of attribute
    * @param options    options for operation
    * @return
    */
  def entityAttributeDetails(entityname: String, attribute: String, options: Map[String, String] = Map()): Try[Map[String, String]] = {
    execute("get details of attribute operation") {
      val properties = definerBlocking.getAttributeProperties(AttributePropertiesMessage(entityname, attribute, options)).properties
      Success(properties)
    }
  }

  /**
    * Get details for index.
    *
    * @param indexname name of index
    * @param options   options for operation
    * @return
    */
  def indexDetails(indexname: String, options: Map[String, String] = Map()): Try[Map[String, String]] = {
    execute("get details of index operation") {
      val properties = definerBlocking.getIndexProperties(IndexPropertiesMessage(indexname, options)).properties
      Success(properties)
    }
  }


  /**
    * Partition entity.
    *
    * @param entityname      name of entity
    * @param npartitions     number of partitions
    * @param attribute       name of attribute
    * @param materialize     materialize partitioning
    * @param replace         replace partitioning
    * @param partitionername partitioner
    * @return
    */
  def entityPartition(entityname: String, npartitions: Int, attribute: Option[String] = None, materialize: Boolean, replace: Boolean, partitionername: String = "spark"): Try[String] = {
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

      val partitioner = partitionername match {
        case "random" => RepartitionMessage.Partitioner.RANDOM
        case "ecp" => RepartitionMessage.Partitioner.ECP
        case "spark" => RepartitionMessage.Partitioner.SPARK
        case _ => RepartitionMessage.Partitioner.SPARK
      }

      val res = definerBlocking.repartitionEntityData(RepartitionMessage(entityname, npartitions, option = option, partitioner = partitioner))

      if (res.code == AckMessage.Code.OK) {
        Success(res.message)
      } else {
        Failure(throw new Exception(res.message))
      }
    }
  }



  /**
    * Transfer storage of entity.
    *
    * @param entityname      name of entity
    * @param attributes      names of attribute
    * @param newhandler       new storage handler
    * @return
    */
  def entityTransferStorage(entityname: String, attributes : Seq[String], newhandler : String): Try[String] = {
    execute("transfer storage of entity operation") {
      val res = definerBlocking.transferStorageHandler(TransferStorageHandlerMessage(entityname, attributes, newhandler))

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
  def entityPreview(entityname: String): Try[Seq[RPCQueryResults]] = {
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
    * @param entityname         name of entity
    * @param attribute          name of feature attribute
    * @param optimizername      optimizer name
    * @param generateNewIndexes generate new indexes
    * @param loggedQueries      use logged queries for test (alternative: use random queries)
    * @param nqueries           number of queries during training phase
    * @param nruns              number of runs per query during training phase
    * @return
    */
  def entityAdaptScanMethods(entityname: String, attribute: String, optimizername: Option[String] = None, generateNewIndexes: Boolean = true, loggedQueries: Boolean = false, nqueries: Option[Int] = None, nruns: Option[Int] = None): Try[Void] = {
    execute("benchmark entity scans and reset weights operation") {

      val ic = if (generateNewIndexes) {
        NEW_INDEXES
      } else {
        EXISTING_INDEXES
      }

      val qc = if (loggedQueries) {
        LOGGED_QUERIES
      } else {
        RANDOM_QUERIES
      }

      var options: Map[String, String] = Map()

      if (qc == RANDOM_QUERIES) {
        options += "nqueries" -> nqueries.getOrElse(100).toString
      } else if (nqueries.isDefined) {
        options += "nqueries" -> nqueries.get.toString
      }

      if (nruns.isDefined) {
        options += "nruns" -> nruns.get.toString
      }

      val optimizer = optimizername.getOrElse("svm") match {
        case "svm" => Optimizer.SVM_OPTIMIZER
        case "naive" => Optimizer.NAIVE_OPTIMIZER
        case "lr" => Optimizer.LR_OPTIMIZER
        case _ => Optimizer.LR_OPTIMIZER
      }

      definerBlocking.adaptScanMethods(AdaptScanMethodsMessage(entityname, attribute, ic, qc, options, optimizer))
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
      definerBlocking.sparsifyEntity(SparsifyEntityMessage(entityname, attribute))
      Success(null)
    }
  }

  /**
    * Vacuum entity.
    *
    * @param entityname name of entity
    */
  def entityVacuum(entityname: String): Try[Void] = {
    execute("vacuum entity operation") {
      definerBlocking.vacuumEntity(EntityNameMessage(entityname))
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
      definerBlocking.dropEntity(EntityNameMessage(entityname))
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
      val res = attributes.map { attribute => definerBlocking.generateAllIndexes(IndexMessage(entity = entityname, attribute = attribute, distance = Some(DistanceMessage(DistanceType.minkowski, options = Map("norm" -> norm.toString)))))
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
      val res = definerBlocking.index(indexMessage)

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
      Success(definerBlocking.listIndexes(EntityNameMessage(entityname)).indexes.map(i => (i.index, i.attribute, i.indextype)))
    }
  }

  /**
    * Check if index exists.
    *
    * @param entityname name of entity
    * @param attribute  nmae of attribute
    * @param indextype  type of index
    * @param acceptStale accept also stale indexes
    * @return
    */
  def indexExists(entityname: String, attribute: String, indextype: String, acceptStale : Boolean = false): Try[Boolean] = {
    execute("index exists operation") {
      val res = definerBlocking.existsIndex(IndexExistsMessage(entityname, attribute, getIndexType(indextype), acceptStale = acceptStale))
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
      val res = definerBlocking.dropIndex(IndexNameMessage(indexname))
      if (res.code.isOk) {
        Success(null)
      } else {
        throw new Exception("dropping index not possible: " + res.message)
      }
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
    * @param indexname       name of index
    * @param npartitions     number of partitions
    * @param attribute       name of attribute
    * @param materialize     materialize partitioning
    * @param replace         replace partitioning
    * @param partitionername partitioner
    * @return
    */
  def indexPartition(indexname: String, npartitions: Int, attribute: Option[String] = None, materialize: Boolean, replace: Boolean, partitionername: String = "spark"): Try[String] = {
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

      val partitioner = partitionername match {
        case "random" => RepartitionMessage.Partitioner.RANDOM
        case "ecp" => RepartitionMessage.Partitioner.ECP
        case "spark" => RepartitionMessage.Partitioner.SPARK
        case _ => RepartitionMessage.Partitioner.SPARK
      }

      val res = definerBlocking.repartitionIndexData(RepartitionMessage(indexname, npartitions, option = option, partitioner = partitioner))

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
  def doQuery(qo: RPCGenericQueryObject): Try[Seq[RPCQueryResults]] = {
    execute("compound query operation") {
      val res = searcherBlocking.doQuery(qo.prepare.buildQueryMessage)
      if (res.ack.get.code.isOk) {
        return Success(res.responses.map(new RPCQueryResults(_)))
      } else {
        throw new Exception(res.ack.get.message)
      }
    }
  }

  /**
    * Perform a search.
    *
    * @param qo search request
    * @param timeout timeout in seconds
    * @return
    */
  def doQuery(qo: RPCGenericQueryObject, timeout: Long): Try[Seq[RPCQueryResults]] = {
    execute("compound query operation") {
      val fut = searcher.doQuery(qo.prepare.buildQueryMessage)

      try {
        val res = Await.result(fut, Duration.apply(timeout, "seconds"))

        if (res.ack.get.code.isOk) {
          return Success(res.responses.map(new RPCQueryResults(_)))
        } else {
          throw new Exception(res.ack.get.message)
        }
      } catch {
        case e: TimeoutException => {
          searcherBlocking.stopQuery(StopQueryMessage(qo.id))
          throw e
        }
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
  def doProgressiveQuery(qo: RPCGenericQueryObject, next: (Try[RPCQueryResults]) => (Unit), completed: (String) => (Unit)): Try[Seq[RPCQueryResults]] = {
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

      searcher.doProgressiveQuery(qo.prepare.buildQueryMessage, so)
      Success(null)
    }
  }

  /**
    * Perform a parallel search.
    *
    * @param qo        search request
    * @param next      function for next result
    * @param completed function for final result
    * @return
    */
  def doParallelQuery(qo: RPCGenericQueryObject, next: (Try[RPCQueryResults]) => (Unit), completed: (String) => (Unit)): Try[Seq[RPCQueryResults]] = {
    execute("parallel query operation") {
      val so = new StreamObserver[QueryResultsMessage]() {
        override def onError(throwable: Throwable): Unit = {
          log.error("error in parallel querying", throwable)
        }

        override def onCompleted(): Unit = {
          completed(qo.id)
        }

        override def onNext(qr: QueryResultsMessage): Unit = {
          log.info("new parallel results arrived")

          if (qr.ack.get.code == AckMessage.Code.OK && qr.responses.nonEmpty) {
            next(Success(new RPCQueryResults(qr.responses.head)))
          } else {
            next(Failure(new Exception(qr.ack.get.message)))
          }
        }
      }

      searcher.doParallelQuery(qo.prepare.buildQueryMessage, so)
      Success(null)
    }
  }

  def getScoredQueryExecutionPaths(qo: RPCGenericQueryObject, optimizername: String = "svm"): Try[Seq[(String, String, Double)]] = {
    execute("collecting scored query execution paths operation") {

      val optimizer  = optimizername match {
        case "svm" => Optimizer.SVM_OPTIMIZER
        case "naive" => Optimizer.NAIVE_OPTIMIZER
        case "lr" => Optimizer.LR_OPTIMIZER
        case _ => throw new Exception("optimizer name is not known")
      }

      val res = searcherBlocking.getScoredExecutionPath(RPCSimulationQueryObject(qo, optimizer).buildQueryMessage)
      if (res.ack.get.code.isOk) {
        return Success(res.executionpaths.map(x => (x.scan, x.scantype, x.score)))
      } else {
        throw new Exception(res.ack.get.message)
      }
    }
  }

  /**
    *
    * @param json
    * @return
    */
  def doQuery(json: String): Try[Seq[RPCQueryResults]] = {
    execute("json query operation") {
      val query = JsonFormat.fromJsonString[QueryMessage](json)
      val res = searcherBlocking.doQuery(query)
      if (res.ack.get.code.isOk) {
        return Success(res.responses.map(new RPCQueryResults(_)))
      } else {
        throw new Exception(res.ack.get.message)
      }
    }
  }



  /**
    * Returns registered storage handlers.
    *
    * @return
    */
  def storageHandlerList(): Try[Map[String, Seq[String]]] = {
    execute("get storage handlers operation") {
      Success(definerBlocking.listStorageHandlers(EmptyMessage()).handlers.map(handler => handler.name -> handler.attributetypes.map(_.toString)).toMap)
    }
  }

  /**
    * Shutdown connection.
    */
  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }


  val str2grpcTypes = Map("auto" -> AttributeType.AUTO, "long" -> AttributeType.LONG, "int" -> AttributeType.INT, "float" -> AttributeType.FLOAT,
    "double" -> AttributeType.DOUBLE, "string" -> AttributeType.STRING, "text" -> AttributeType.TEXT, "boolean" -> AttributeType.BOOLEAN, "geography" -> AttributeType.GEOGRAPHY,
    "geometry" -> AttributeType.GEOMETRY, "vector" -> AttributeType.VECTOR, "sparsevector" -> AttributeType.SPARSEVECTOR)

  val grpc2strTypes = str2grpcTypes.map(_.swap)

  /**
    *
    * @param s string of field type name
    * @return
    */
  private def getGrpcType(s: String): AttributeType = str2grpcTypes.get(s).orNull

  private def getStrType(a: AttributeType): String = grpc2strTypes.get(a).orNull

  //TODO: add get attributes-method for an entity, to retrieve attributes to display
}

object RPCClient {
  def apply(host: String, port: Int): RPCClient = {
    val channel = NettyChannelBuilder.forAddress(host, port)
      .usePlaintext(true)
      .nameResolverFactory(new DnsNameResolverProvider())
      .maxMessageSize(12582912)
      .asInstanceOf[ManagedChannelBuilder[_]]
      .build()

    new RPCClient(
      channel,
      AdamDefinitionGrpc.blockingStub(channel),
      AdamDefinitionGrpc.stub(channel),
      AdamSearchGrpc.blockingStub(channel),
      AdamSearchGrpc.stub(channel)
    )
  }
}