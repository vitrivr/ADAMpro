package org.vitrivr.adampro.rpc

import io.grpc.stub.StreamObserver
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.vitrivr.adampro.api._
import org.vitrivr.adampro.entity.{AttributeNameHolder, Entity}
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.grpc.grpc.AdaptScanMethodsMessage.{IndexCollection, QueryCollection}
import org.vitrivr.adampro.grpc.grpc._
import org.vitrivr.adampro.query.optimizer.IndexCollectionFactory.{ExistingIndexCollectionOption, NewIndexCollectionOption}
import org.vitrivr.adampro.query.optimizer.QueryCollectionFactory.{LoggedQueryCollectionOption, RandomQueryCollectionOption}
import org.vitrivr.adampro.query.optimizer._
import org.vitrivr.adampro.index.partition.{PartitionMode, PartitionerChoice}
import org.vitrivr.adampro.helpers.storage.Transferer
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.query.query.Predicate
import org.vitrivr.adampro.utils.{AdamImporter, Logging, ProtoImporterExporter}

import scala.concurrent.Future

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class DataDefinitionRPC extends AdamDefinitionGrpc.AdamDefinition with Logging {
  implicit def ac: AdamContext = SparkStartup.mainContext

  /**
    *
    * @param request
    * @return
    */
  override def createEntity(request: CreateEntityMessage): Future[AckMessage] = {
    log.debug("rpc call for create entity operation")
    val entityname = request.entity

    val attributes = RPCHelperMethods.prepareAttributes(request.attributes)
    val res = EntityOp.create(entityname, attributes)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK, res.get.entityname))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def availableAttributeTypes(request: EmptyMessage): Future[AvailableAttributeTypesMessage] = {
    //TODO: implement
    Future.successful(AvailableAttributeTypesMessage(Some(AckMessage(code = AckMessage.Code.OK)), AttributeType.values))
  }


  /**
    *
    * @param request
    * @return
    */
  override def existsEntity(request: EntityNameMessage): Future[ExistsMessage] = {
    log.debug("rpc call for entity exists operation")
    val res = EntityOp.exists(request.entity)

    if (res.isSuccess) {
      Future.successful(ExistsMessage(Some(AckMessage(code = AckMessage.Code.OK)), res.get))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(ExistsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def sparsifyEntity(request: SparsifyEntityMessage): Future[AckMessage] = {
    log.debug("rpc call for compress operation")
    val res = EntityOp.sparsify(request.entity, request.attribute)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def count(request: EntityNameMessage): Future[AckMessage] = {
    log.debug("rpc call for count entity operation")
    val res = EntityOp.count(request.entity)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK, res.get.toString))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def insert(request: InsertMessage): Future[AckMessage] = {
    log.debug("rpc call for insert operation")

    //TODO: remove code duplication with streamInsert
    val entity = Entity.load(request.entity)

    if (entity.isFailure) {
      return Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = "cannot load entity"))
    }

    val schema = entity.get.schema(fullSchema = false)

    val rows = request.tuples.map(tuple => {
      val data = schema.map(field => {
        val datum = tuple.data.get(field.name).getOrElse(null)
        if (datum != null) {
          RPCHelperMethods.prepareDataTypeConverter(field.attributeType)(datum)
        } else {
          null
        }
      })
      Row(data: _*)
    })

    val rdd = ac.sc.parallelize(rows)
    val df = ac.sqlContext.createDataFrame(rdd, StructType(entity.get.schema(fullSchema = false).map(field => StructField(field.name, field.attributeType.datatype))))

    val res = EntityOp.insert(entity.get.entityname, df)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } else {
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }


  /**
    *
    * @param responseObserver
    * @return
    */
  override def streamInsert(responseObserver: StreamObserver[AckMessage]): StreamObserver[InsertMessage] = {
    new StreamObserver[InsertMessage]() {

      def onNext(request: InsertMessage) {
        val entity = Entity.load(request.entity)

        if (entity.isFailure) {
          return onError(new GeneralAdamException("cannot load entity"))
        }

        val schema = entity.get.schema(fullSchema = false)

        val rows = request.tuples.map(tuple => {
          val data = schema.map(field => {
            val datum = tuple.data.get(field.name).getOrElse(null)
            if (datum != null) {
              RPCHelperMethods.prepareDataTypeConverter(field.attributeType)(datum)
            } else {
              null
            }
          })
          Row(data: _*)
        })

        val rdd = ac.sc.parallelize(rows)
        val df = ac.sqlContext.createDataFrame(rdd, StructType(entity.get.schema(fullSchema = false).map(field => StructField(field.name, field.attributeType.datatype))))

        val res = EntityOp.insert(entity.get.entityname, df)

        if (res.isSuccess) {
          responseObserver.onNext(AckMessage(code = AckMessage.Code.OK))
        } else {
          responseObserver.onNext(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
        }
      }

      def onError(t: Throwable) = {
        log.error(t.getMessage)
        responseObserver.onNext(AckMessage(code = AckMessage.Code.ERROR, message = t.getMessage))
      }

      def onCompleted() = {
        responseObserver.onCompleted()
      }
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def vacuumEntity(request: EntityNameMessage): Future[AckMessage] = {
    log.debug("rpc call for vacuum operation")

    val entityname = request.entity

    val res = EntityOp.vacuum(entityname)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def delete(request: DeleteMessage): Future[AckMessage] = {
    log.debug("rpc call for delete operation")

    val predicates = request.predicates.map(bqm => {
      val attribute = bqm.attribute
      val op = if (bqm.op.isEmpty) {
        None
      } else {
        Some(bqm.op)
      }
      val values = bqm.values.map(value => value.datatype.number match {
        case DataMessage.BOOLEANDATA_FIELD_NUMBER => value.getBooleanData
        case DataMessage.DOUBLEDATA_FIELD_NUMBER => value.getBooleanData
        case DataMessage.FLOATDATA_FIELD_NUMBER => value.getBooleanData
        case DataMessage.GEOGRAPHYDATA_FIELD_NUMBER => value.getGeographyData
        case DataMessage.GEOMETRYDATA_FIELD_NUMBER => value.getGeometryData
        case DataMessage.INTDATA_FIELD_NUMBER => value.getIntData
        case DataMessage.LONGDATA_FIELD_NUMBER => value.getLongData
        case DataMessage.STRINGDATA_FIELD_NUMBER => value.getStringData
        case _ => throw new GeneralAdamException("search predicates can not be of any type")
      })

      new Predicate(bqm.attribute, op, values)
    })

    val res = EntityOp.delete(request.entity, predicates)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK, message = res.get.toString))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def index(request: IndexMessage): Future[AckMessage] = {
    log.debug("rpc call for indexing operation")
    val indextypename = IndexTypes.withIndextype(request.indextype)

    if (indextypename.isEmpty) {
      return Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = "index type not existing"))
    }

    val distance = RPCHelperMethods.prepareDistance(request.distance)

    val res = IndexOp.create(request.entity, request.attribute, indextypename.get, distance, request.options)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK, message = res.get.indexname))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def dropEntity(request: EntityNameMessage): Future[AckMessage] = {
    log.debug("rpc call for dropping entity operation")
    val res = EntityOp.drop(request.entity)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def existsIndex(request: IndexMessage): Future[ExistsMessage] = {
    log.debug("rpc call for index exists operation")
    val res = IndexOp.exists(request.entity)

    if (res.isSuccess) {
      Future.successful(ExistsMessage(Some(AckMessage(code = AckMessage.Code.OK)), res.get))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(ExistsMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def dropIndex(request: IndexNameMessage): Future[AckMessage] = {
    log.debug("rpc call for dropping index operation")
    val res = IndexOp.drop(request.index)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def listIndexes(request: EntityNameMessage): Future[IndexesMessage] = {
    log.debug("rpc call for listing indexes")

    val res = if(request.entity != null && request.entity.nonEmpty){
      IndexOp.list(request.entity)
    } else {
      IndexOp.list()
    }

    if (res.isSuccess) {
      Future.successful(IndexesMessage(Some(AckMessage(AckMessage.Code.OK)), res.get.map(r => IndexesMessage.IndexMessage(r._1, r._2, r._3.indextype))))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(IndexesMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def generateRandomData(request: GenerateRandomDataMessage): Future[AckMessage] = {
    log.debug("rpc call for creating random data")

    val res = RandomDataOp(request.entity, request.ntuples, request.options)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def listEntities(request: EmptyMessage): Future[EntitiesMessage] = {
    log.debug("rpc call for listing entities")
    val res = EntityOp.list()

    if (res.isSuccess) {
      Future.successful(EntitiesMessage(Some(AckMessage(AckMessage.Code.OK)), res.get.map(_.toString())))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(EntitiesMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def getEntityProperties(request: EntityPropertiesMessage): Future[PropertiesMessage] = {
    log.debug("rpc call for returning entity properties")
    val res = EntityOp.properties(request.entity, options = request.options)

    if (res.isSuccess) {
      Future.successful(PropertiesMessage(Some(AckMessage(AckMessage.Code.OK)), request.entity, res.get))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(PropertiesMessage(Some(AckMessage(AckMessage.Code.ERROR, res.failed.get.getMessage))))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def getAttributeProperties(request: AttributePropertiesMessage): Future[PropertiesMessage] = {
    log.debug("rpc call for returning attribute properties")
    val res = EntityOp.properties(request.entity, Some(request.attribute), options = request.options)

    if (res.isSuccess) {
      Future.successful(PropertiesMessage(Some(AckMessage(AckMessage.Code.OK)), request.entity, res.get))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(PropertiesMessage(Some(AckMessage(AckMessage.Code.ERROR, res.failed.get.getMessage))))
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def getIndexProperties(request: IndexPropertiesMessage): Future[PropertiesMessage] = {
    log.debug("rpc call for returning index properties")
    val res = IndexOp.properties(request.index, request.options)

    if (res.isSuccess) {
      Future.successful(PropertiesMessage(Some(AckMessage(AckMessage.Code.OK)), request.index, res.get))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(PropertiesMessage(Some(AckMessage(AckMessage.Code.ERROR, res.failed.get.getMessage))))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def repartitionIndexData(request: RepartitionMessage): Future[AckMessage] = {
    log.debug("rpc call for repartitioning index")

    val attribute = if (request.attributes.isEmpty) {
      None
    } else {
      Some(AttributeNameHolder(request.attributes))
    }

    val option = request.option match {
      case RepartitionMessage.PartitionOptions.CREATE_NEW => PartitionMode.CREATE_NEW
      case RepartitionMessage.PartitionOptions.CREATE_TEMP => PartitionMode.CREATE_TEMP
      case RepartitionMessage.PartitionOptions.REPLACE_EXISTING => PartitionMode.REPLACE_EXISTING
      case _ => PartitionMode.CREATE_NEW
    }

    //Note that default is spark
    val partitioner = request.partitioner match {
      case RepartitionMessage.Partitioner.SPARK => PartitionerChoice.SPARK
      case RepartitionMessage.Partitioner.RANDOM => PartitionerChoice.RANDOM
      case RepartitionMessage.Partitioner.ECP => PartitionerChoice.ECP
      case _ => PartitionerChoice.SPARK
    }

    val res = IndexOp.partition(request.entity, request.numberOfPartitions, None, attribute, option, partitioner, request.options)

    if (res.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK, res.get.indexname))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def repartitionEntityData(request: RepartitionMessage): Future[AckMessage] = {
    log.debug("rpc call for repartitioning entity")

    val attribute = if (request.attributes.isEmpty) {
      None
    } else {
      Some(request.attributes)
    }

    val option = request.option match {
      case RepartitionMessage.PartitionOptions.CREATE_NEW => PartitionMode.CREATE_NEW
      case RepartitionMessage.PartitionOptions.CREATE_TEMP => PartitionMode.CREATE_TEMP
      case RepartitionMessage.PartitionOptions.REPLACE_EXISTING => PartitionMode.REPLACE_EXISTING
      case _ => PartitionMode.CREATE_NEW
    }

    val res = EntityOp.partition(request.entity, request.numberOfPartitions, None, attribute.map(AttributeNameHolder(_)), option)

    if (res.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK, res.get.entityname))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def adaptScanMethods(request: AdaptScanMethodsMessage): Future[AckMessage] = {
    log.debug("rpc call for benchmarking entity and index")

    val ico = request.ic match {
      case IndexCollection.EXISTING_INDEXES => ExistingIndexCollectionOption
      case IndexCollection.NEW_INDEXES => NewIndexCollectionOption
      case _ => null
    }
    val ic = IndexCollectionFactory(request.entity, request.attribute, ico, request.options)


    val qco = request.qc match {
      case QueryCollection.LOGGED_QUERIES => LoggedQueryCollectionOption
      case QueryCollection.RANDOM_QUERIES => RandomQueryCollectionOption
      case _ => null
    }
    val qc = QueryCollectionFactory(request.entity, request.attribute, qco, request.options)


    val res1 = ac.optimizerRegistry.value.apply("naive").get.train(ic, qc)
    val res2 = ac.optimizerRegistry.value.apply("svm").get.train(ic, qc)

    if (res1.isSuccess && res2.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK, request.entity))
    } else if(res1.isSuccess) {
      log.error(res2.failed.get.getMessage, res2.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res2.failed.get.getMessage))
    } else if(res2.isSuccess) {
      log.error(res1.failed.get.getMessage, res1.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res1.failed.get.getMessage))
    } else {
      log.error(res1.failed.get.getMessage, res1.failed.get)
      log.error(res2.failed.get.getMessage, res2.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res1.failed.get.getMessage + " " + res2.failed.get.getMessage))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def generateAllIndexes(request: IndexMessage): Future[AckMessage] = {
    log.debug("rpc call for generating all indexes")

    val distance = RPCHelperMethods.prepareDistance(request.distance)
    val res = IndexOp.generateAll(request.entity, request.attribute, distance)

    if (res.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK, res.get.mkString(",")))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  @Experimental override def importData(request: ImportMessage): Future[AckMessage] = {
    log.debug("rpc call for importing data from old ADAM")
    val res = AdamImporter(request.host, request.database, request.username, request.password)
    if (res.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  @Experimental override def protoImportData(request: ProtoImportMessage, responseObserver: StreamObserver[AckMessage]): Unit = {
    log.debug("rpc call for importing data from proto files")
    new ProtoImporterExporter().importData(request.path, createEntity, insert, responseObserver)
  }


  /**
    *
    * @param request
    * @return
    */
  @Experimental override def protoExportData(request: ProtoExportMessage): Future[AckMessage] = {
    log.debug("rpc call for importing data from proto files")

    val entity = Entity.load(request.entity)

    if (entity.isFailure) {
      return Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = "cannot load entity"))
    }

    val res = new ProtoImporterExporter().exportData(request.path, entity.get)

    if (res.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }


  /**
    *
    * @param request
    * @return
    */
  override def listStorageHandlers(request: EmptyMessage): Future[StorageHandlersMessage] = {
    log.debug("rpc call for listing storage handlers")

    val handlers = SparkStartup.mainContext.storageHandlerRegistry.value.handlers.filterNot(_._2.supports.isEmpty).map(handler => handler._1 -> handler._2.supports.map(RPCHelperMethods.getGrpcType(_)))

    Future.successful(StorageHandlersMessage(handlers.map(handler => StorageHandlerMessage(handler._1, handler._2)).toSeq))
  }

  /**
    *
    * @param request
    * @return
    */
  override def transferStorageHandler(request: TransferStorageHandlerMessage): Future[AckMessage] = {
    log.debug("rpc call for transfering storage handler")

    val entity = Entity.load(request.entity)

    if (entity.isFailure) {
      return Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = "cannot load entity"))
    }

    val res = Transferer(entity.get, request.attributes, request.handler)

    if (res.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }
}