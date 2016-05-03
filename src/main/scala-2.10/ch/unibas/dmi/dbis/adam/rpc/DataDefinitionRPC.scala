package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.api._
import ch.unibas.dmi.dbis.adam.entity.{EntityHandler, FieldDefinition, FieldTypes}
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.http.grpc.FieldDefinitionMessage.FieldType
import ch.unibas.dmi.dbis.adam.http.grpc.{AckMessage, CreateEntityMessage, _}
import ch.unibas.dmi.dbis.adam.index.IndexHandler
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.partitions.{PartitionHandler, PartitionOptions}
import io.grpc.stub.StreamObserver
import org.apache.log4j.Logger
import org.apache.spark.sql.Row

import scala.concurrent.Future

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class DataDefinitionRPC(implicit ac: AdamContext) extends AdamDefinitionGrpc.AdamDefinition {
  val log = Logger.getLogger(getClass.getName)

  override def createEntity(request: CreateEntityMessage): Future[AckMessage] = {
    log.debug("rpc call for create entity operation")

    val entityname = request.entity
    val fields = request.fields.map(field => {
      FieldDefinition(field.name, matchFields(field.fieldtype), false, field.unique, field.indexed)
    })
    val entity = CreateEntityOp(entityname, fields)

    if (entity.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK, entity.get.entityname))
    } else {
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = entity.failed.get.getMessage))
    }
  }

  /**
    *
    * @param ft
    * @return
    */
  private def matchFields(ft: FieldDefinitionMessage.FieldType) = ft match {
    case FieldType.BOOLEAN => FieldTypes.BOOLEANTYPE
    case FieldType.DOUBLE => FieldTypes.DOUBLETYPE
    case FieldType.FLOAT => FieldTypes.FLOATTYPE
    case FieldType.INT => FieldTypes.INTTYPE
    case FieldType.LONG => FieldTypes.LONGTYPE
    case FieldType.STRING => FieldTypes.STRINGTYPE
    case FieldType.FEATURE => FieldTypes.FEATURETYPE
    case _ => FieldTypes.UNRECOGNIZEDTYPE
  }


  override def count(request: EntityNameMessage): Future[AckMessage] = {
    log.debug("rpc call for count entity operation")

    val count = CountOp(request.entity)

    if (count.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK, count.get.toString))
    } else {
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = count.failed.get.getMessage))
    }
  }


  override def insert(responseObserver: StreamObserver[AckMessage]): StreamObserver[InsertMessage] = {
    new StreamObserver[InsertMessage]() {
      def onNext(insert: InsertMessage) {
        val entity = EntityHandler.load(insert.entity)

        if (entity.isFailure) {
          return onError(new GeneralAdamException("cannot load entity"))
        }

        val schema = entity.get.schema


        val rows = insert.tuples.map(tuple => {
          val metadata = tuple.metadata
          val data = schema.map(field => metadata.get(field.name).get).+:(tuple.vector)
          Row(data: _*)
        })

        val rdd = ac.sc.parallelize(rows)
        val df = ac.sqlContext.createDataFrame(rdd, entity.get.schema)

        InsertOp(entity.get.entityname, df)

        responseObserver.onNext(AckMessage(code = AckMessage.Code.OK))
      }

      def onError(t: Throwable) = {
        responseObserver.onNext(AckMessage(code = AckMessage.Code.ERROR))
        log.error("error on insertion", t)
        responseObserver.onError(t)
      }

      def onCompleted() = {
        responseObserver.onCompleted()
      }
    }
  }


  override def index(request: IndexMessage): Future[AckMessage] = {
    log.debug("rpc call for indexing operation")

    val indextypename = IndexTypes.withIndextype(request.indextype)

    if (indextypename.isEmpty) {
      throw new Exception("no index type name given.")
    }

    val index = IndexOp(request.entity, request.column, indextypename.get, RPCHelperMethods.prepareDistance(request.distance.get), request.options)

    if (index.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK, message = index.get.indexname))
    } else {
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = index.failed.get.getMessage))
    }
  }

  override def dropEntity(request: EntityNameMessage): Future[AckMessage] = {
    log.debug("rpc call for dropping entity operation")

    val drop = DropEntityOp(request.entity)

    if (drop.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } else {
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = drop.failed.get.getMessage))
    }
  }


  override def dropIndex(request: IndexNameMessage): Future[AckMessage] = {
    log.debug("rpc call for dropping index operation")

    val drop = DropIndexOp(request.index)

    if (drop.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } else {
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = drop.failed.get.getMessage))
    }
  }


  override def generateRandomData(request: GenerateRandomDataMessage): Future[AckMessage] = {
    log.debug("rpc call for creating random data")

    try {
      RandomDataOp(request.entity, request.ntuples, request.ndims)
      log.info("genereated random data")
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } catch {
      case e: Exception =>
        log.debug("exception while rpc call for creating random data")
        Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))
    }
  }


  override def listEntities(request: Empty): Future[EntitiesMessage] = {
    log.debug("rpc call for listing entities")
    Future.successful(EntitiesMessage(Some(AckMessage(AckMessage.Code.OK)), ListEntitiesOp().map(_.toString())))
  }


  override def getEntityProperties(request: EntityNameMessage): Future[EntityPropertiesMessage] = {
    log.debug("rpc call for returning entity properties")
    val properties = EntityHandler.getProperties(request.entity)

    if (properties.isSuccess) {
      Future.successful(EntityPropertiesMessage(Some(AckMessage(AckMessage.Code.OK)), request.entity, properties.get))
    } else {
      Future.successful(EntityPropertiesMessage(Some(AckMessage(AckMessage.Code.ERROR, properties.failed.get.getMessage))))
    }
  }

  override def repartitionIndexData(request: RepartitionMessage): Future[AckMessage] = {
    log.debug("rpc call for repartitioning index")

    val cols = if (request.columns.isEmpty) {
      None
    } else {
      Some(request.columns)
    }

    val option = request.option match {
      case RepartitionMessage.PartitionOptions.CREATE_NEW => PartitionOptions.CREATE_NEW
      case RepartitionMessage.PartitionOptions.CREATE_TEMP => PartitionOptions.CREATE_TEMP
      case RepartitionMessage.PartitionOptions.REPLACE_EXISTING => PartitionOptions.REPLACE_EXISTING
      case _ => PartitionOptions.CREATE_NEW
    }

    val index = PartitionHandler.repartitionIndex(request.index, request.numberOfPartitions, request.useMetadataForPartitioning, cols, option)

    if (index.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK, index.get.indexname))
    } else {
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = index.failed.get.getMessage))
    }
  }

  override def setIndexWeight(request: IndexWeightMessage): Future[AckMessage] = {
    log.debug("rpc call for changing weight of index")

    if (IndexHandler.setWeight(request.index, request.weight)) {
      Future.successful(AckMessage(AckMessage.Code.OK, request.index))
    } else {
      Future.successful(AckMessage(AckMessage.Code.ERROR, "please try again"))
    }
  }

  override def generateAllIndexes(request: IndexMessage): Future[AckMessage] = {
    val res = IndexOp.generateAll(request.entity, request.column, RPCHelperMethods.prepareDistance(request.distance.get))

    if (res) {
      Future.successful(AckMessage(AckMessage.Code.OK))
    } else {
      Future.successful(AckMessage(AckMessage.Code.ERROR))
    }
  }
}