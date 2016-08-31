package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.api._
import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.helpers.partition.{PartitionMode, PartitionerChoice}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
import ch.unibas.dmi.dbis.adam.utils.{AdamImporter, Logging}
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.vitrivr.adam.grpc.grpc._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

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
    val res = EntityOp(entityname, attributes)

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
    * @param responseObserver
    * @return
    */
  override def insert(responseObserver: StreamObserver[AckMessage]): StreamObserver[InsertMessage] = {
    new StreamObserver[InsertMessage]() {

      def onNext(insert: InsertMessage) {
        val entity = Entity.load(insert.entity)

        if (entity.isFailure) {
          return onError(new GeneralAdamException("cannot load entity"))
        }

        val schema = entity.get.schema()

        val rows = insert.tuples.map(tuple => {
          val data = schema.map(field => {
            val datum = tuple.data.get(field.name).getOrElse(null)
            if (datum != null) {
              RPCHelperMethods.prepareDataTypeConverter(field.fieldtype.datatype)(datum)
            } else {
              null
            }
          })
          Row(data: _*)
        })

        val rdd = ac.sc.parallelize(rows)
        val df = ac.sqlContext.createDataFrame(rdd, StructType(entity.get.schema().map(field => StructField(field.name, field.fieldtype.datatype))))

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
  override def index(request: IndexMessage): Future[AckMessage] = {
    log.debug("rpc call for indexing operation")
    val indextypename = IndexTypes.withIndextype(request.indextype)

    if (indextypename.isEmpty) {
      return Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = "index type not existing"))
    }

    val distance = RPCHelperMethods.prepareDistance(request.distance)

    val res = IndexOp(request.entity, request.attribute, indextypename.get, distance, request.options)

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
    val res = IndexOp.list(request.entity)

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
  override def getEntityProperties(request: EntityNameMessage): Future[EntityPropertiesMessage] = {
    log.debug("rpc call for returning entity properties")
    val res = EntityOp.properties(request.entity)

    if (res.isSuccess) {
      Future.successful(EntityPropertiesMessage(Some(AckMessage(AckMessage.Code.OK)), request.entity, res.get))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(EntityPropertiesMessage(Some(AckMessage(AckMessage.Code.ERROR, res.failed.get.getMessage))))
    }
  }

  /**
    *
    * @param request
    * @return
    */
  override def repartitionIndexData(request: RepartitionMessage): Future[AckMessage] = {
    log.debug("rpc call for repartitioning index")

    val cols = if (request.attributes.isEmpty) {
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

    //Note that default is spark
    val partitioner = request.partitioner match {
      case RepartitionMessage.Partitioner.SPARK => PartitionerChoice.SPARK
      case RepartitionMessage.Partitioner.CURRENT => PartitionerChoice.CURRENT
      case RepartitionMessage.Partitioner.RANDOM => PartitionerChoice.RANDOM
      case _ => PartitionerChoice.SPARK
    }

    val res = IndexOp.partition(request.entity, request.numberOfPartitions, None, cols, option, partitioner, request.options)

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

    val cols = if (request.attributes.isEmpty) {
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

    val res = EntityOp.partition(request.entity, request.numberOfPartitions, None, cols, option)

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
  override def setScanWeight(request: WeightMessage): Future[AckMessage] = {
    log.debug("rpc call for changing weight of entity or index")

    val res = if (CatalogOperator.existsEntity(request.entity).get) {
      EntityOp.setScanWeight(request.entity, request.attribute, request.weight)
    } else {
      IndexOp.setScanWeight(request.entity, request.weight)
    }

    if (res.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK, request.entity))
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
  override def adjustScanWeights(request: UpdateWeightsMessage): Future[AckMessage] = {
    log.debug("rpc call for benchmarking entity and index")
    val res = EntityOp.adjustScanWeights(request.entity, request.attribute, request.benchmark)

    if (res.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK, request.entity))
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
  override def resetScanWeights(request: EntityNameMessage): Future[AckMessage] = {
    log.debug("rpc call for resetting entity and index scan weights")
    val res = EntityOp.resetScanWeights(request.entity)

    if (res.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK, request.entity))
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
  @Experimental override def importDataFile(request: ImportDataFileMessage): Future[AckMessage] = {
    log.debug("rpc call for importing entity from file")

    //create entity if necessary
    val entityname = if (request.destination.isCreateEntity || request.destination.isDefinitionfile) {
      val fut = if (request.destination.isCreateEntity) {
        createEntity(request.getCreateEntity)
      } else {
        createEntity(CreateEntityMessage.parseFrom(request.getDefinitionfile.toByteArray))
      }
      val res = Await.result(fut, 100.seconds)

      if (res.code.isError) {
        return Future.successful(res)
      }

      request.getCreateEntity.entity
    } else {
      request.getEntity
    }


    val filetype = request.filetype //not used at the moment
    val data = request.datafile.toByteArray

    val protoie = new ProtoImporterExporter(entityname)
    val res = protoie.importData(data)

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
  @Experimental override def exportDataFile(request: EntityNameMessage): Future[ExportDataFileMessage] = {
    log.debug("rpc call for exporting entity to file")
    val protoie = new ProtoImporterExporter(request.entity)
    val res = protoie.exportData()

    if (res.isSuccess) {
      Future.successful(ExportDataFileMessage(Some(AckMessage(code = AckMessage.Code.OK)), ByteString.copyFrom(res.get._1), ByteString.copyFrom(res.get._2)))
    } else {
      log.error(res.failed.get.getMessage, res.failed.get)
      Future.successful(ExportDataFileMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
    }
  }
}