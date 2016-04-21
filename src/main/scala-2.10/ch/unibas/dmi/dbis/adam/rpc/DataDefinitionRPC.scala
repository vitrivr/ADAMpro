package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.api._
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.{EntityHandler, FieldDefinition, FieldTypes}
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.http.grpc.FieldDefinitionMessage.FieldType
import ch.unibas.dmi.dbis.adam.http.grpc.{AckMessage, CreateEntityMessage, _}
import ch.unibas.dmi.dbis.adam.index.IndexHandler
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
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

    try {
      if (!request.fields.isEmpty) {
        val entityname = request.entity
        val fields = request.fields.map(field => {
          FieldDefinition(field.name, matchFields(field.fieldtype), false, field.unique, field.indexed)
        })
        CreateEntityOp(entityname, Option(fields))
      } else {
        if (request.fields.contains(FieldNames.idColumnName)
          || request.fields.contains(FieldNames.internFeatureColumnName)
          || request.fields.contains(FieldNames.distanceColumnName)) {
          throw new GeneralAdamException("Field specification contains reserved name.")
        }

        CreateEntityOp(request.entity, None)
      }
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } catch {
      case e: Exception =>
        log.debug("exception while rpc call for create entity operation")
        Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))
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
    case _ => FieldTypes.UNRECOGNIZEDTYPE
  }


  override def count(request: EntityNameMessage): Future[AckMessage] = {
    log.debug("rpc call for count entity operation")

    try {
      val count = CountOp(request.entity)
      Future.successful(AckMessage(code = AckMessage.Code.OK, message = count.toString))
    } catch {
      case e: Exception =>
        log.debug("exception while rpc call for count entity operation")
        Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))
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

      def onError(t: Throwable) {
        responseObserver.onNext(AckMessage(code = AckMessage.Code.ERROR))
        log.error("error on insertion", t)
        responseObserver.onError(t)
      }

      def onCompleted(): Unit = {
        responseObserver.onCompleted()
      }
    }
  }


  override def index(request: IndexMessage): Future[AckMessage] = {
    log.debug("rpc call for indexing operation")

    try {
      val indextypename = request.indextype match {
        case IndexType.ecp => IndexTypes.ECPINDEX
        case IndexType.sh => IndexTypes.SHINDEX
        case IndexType.lsh => IndexTypes.LSHINDEX
        case IndexType.vaf => IndexTypes.VAFINDEX
        case IndexType.vav => IndexTypes.VAVINDEX
        case _ => null
      }

      if (indextypename == null) {
        throw new Exception("no index type name given.")
      }

      val index = IndexOp(request.entity, indextypename, RPCHelperMethods.prepareDistance(request.distance.get), request.options)
      Future.successful(AckMessage(code = AckMessage.Code.OK, message = index.get.indexname))
    } catch {
      case e: Exception =>
        log.debug("exception while rpc call for indexing operation")
        Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))
    }
  }

  override def dropEntity(request: EntityNameMessage): Future[AckMessage] = {
    log.debug("rpc call for dropping entity operation")

    try {
      DropEntityOp(request.entity)
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } catch {
      case e: Exception =>
        log.debug("exception while rpc call for dropping entity operation")
        Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))
    }
  }


  override def dropIndex(request: IndexNameMessage): Future[AckMessage] = {
    log.debug("rpc call for dropping index operation")

    try {
      DropIndexOp(request.index)
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } catch {
      case e: Exception =>
        log.debug("exception while rpc call for dropping index operation")
        Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))
    }
  }


  override def prepareForDemo(request: GenerateRandomEntityMessage): Future[AckMessage] = {
    log.debug("rpc call for creating random data")

    try {
      val fields = if (!request.fields.isEmpty) {
        Some(
          request.fields.map(fd =>
            FieldDefinition(fd.name, matchFields(fd.fieldtype), fd.pk, fd.unique, fd.indexed)
          )
        )
      } else {
        None
      }

      RandomDataOp(request.entity, request.ntuples, request.ndims, fields)
      log.info("genereated random data")
      IndexOp.generateAll(request.entity, EuclideanDistance)
      log.info("genereated indices to random data")
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } catch {
      case e: Exception =>
        log.debug("exception while rpc call for creating random data")
        Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))
    }
  }


  override def listEntities(request: Empty): Future[EntitiesMessage] = {
    log.debug("rpc call for listing entities")
    Future.successful(EntitiesMessage(ListEntitiesOp().map(_.toString())))
  }


  override def getEntityProperties(request: EntityNameMessage): Future[EntityPropertiesMessage] = {
    log.debug("rpc call for returning entity properties")
    val properties = EntityHandler.getProperties(request.entity)

    if (properties.isSuccess) {
      Future.successful(EntityPropertiesMessage(request.entity, properties.get))
    } else {
      Future.failed(properties.failed.get)
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
      log.debug("exception while rpc call for repartitioning index")
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
}