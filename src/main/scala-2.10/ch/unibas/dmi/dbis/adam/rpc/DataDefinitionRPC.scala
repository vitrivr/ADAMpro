package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.api._
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.entity.{Entity, FieldDefinition, FieldTypes}
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.http.grpc.FieldDefinitionMessage.FieldType
import ch.unibas.dmi.dbis.adam.http.grpc.{AckMessage, CreateEntityMessage, _}
import ch.unibas.dmi.dbis.adam.index.PartitionMode
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import io.grpc.stub.StreamObserver
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StructField, StructType, DataType}
import org.apache.spark.sql.{Row, types}

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
      FieldDefinition(field.name, matchFields(field.fieldtype), field.pk, field.unique, field.indexed)
    })
    val res = EntityOp(entityname, fields)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK, res.get.entityname))
    } else {
      log.error(res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
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

  private def converter(datatype: DataType): (InsertDataMessage) => (Any) = datatype match {
    case types.BooleanType => (x) => x.getBooleanData
    case types.DoubleType => (x) => x.getBooleanData
    case types.FloatType => (x) => x.getFloatData
    case types.IntegerType => (x) => x.getIntData
    case types.LongType => (x) => x.getLongData
    case types.StringType => (x) => x.getStringData
    case _: FeatureVectorWrapperUDT => (x) => FeatureVectorWrapper(RPCHelperMethods.prepareFeatureVector(x.getFeatureData))
  }


  override def count(request: EntityNameMessage): Future[AckMessage] = {
    log.debug("rpc call for count entity operation")
    val res = EntityOp.count(request.entity)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK, res.get.toString))
    } else {
      log.error(res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }


  override def insert(responseObserver: StreamObserver[AckMessage]): StreamObserver[InsertMessage] = {
    new StreamObserver[InsertMessage]() {

      def onNext(insert: InsertMessage) {
        val entity = Entity.load(insert.entity)

        if (entity.isFailure) {
          return onError(new GeneralAdamException("cannot load entity"))
        }

        val schema = entity.get.schema

        val rows = insert.tuples.map(tuple => {
          val data = schema.map(field => {
            val datum = tuple.data.get(field.name).getOrElse(null)
            if (datum != null) {
              converter(field.fieldtype.datatype)(datum)
            } else {
              null
            }
          })
          Row(data: _*)
        })

        val rdd = ac.sc.parallelize(rows)
        val df = ac.sqlContext.createDataFrame(rdd, StructType(entity.get.schema.map(field => StructField(field.name, field.fieldtype.datatype))))

        val res = EntityOp.insert(entity.get.entityname, df)

        if (res.isSuccess) {
          responseObserver.onNext(AckMessage(code = AckMessage.Code.OK))
        } else {
          responseObserver.onNext(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
        }
      }

      def onError(t: Throwable) = {
        log.error(t)
        responseObserver.onNext(AckMessage(code = AckMessage.Code.ERROR, message = t.getMessage))
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
      return Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = "index type not existing"))
    }

    val res = IndexOp(request.entity, request.column, indextypename.get, RPCHelperMethods.prepareDistance(request.distance.get), request.options)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK, message = res.get.indexname))
    } else {
      log.error(res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }

  override def dropEntity(request: EntityNameMessage): Future[AckMessage] = {
    log.debug("rpc call for dropping entity operation")
    val res = EntityOp.drop(request.entity)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } else {
      log.error(res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }


  override def dropIndex(request: IndexNameMessage): Future[AckMessage] = {
    log.debug("rpc call for dropping index operation")
    val res = IndexOp.drop(request.index)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } else {
      log.error(res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }


  override def generateRandomData(request: GenerateRandomDataMessage): Future[AckMessage] = {
    log.debug("rpc call for creating random data")
    val res = RandomDataOp(request.entity, request.ntuples, request.ndims)

    if (res.isSuccess) {
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } else {
      log.error(res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }


  override def listEntities(request: EmptyMessage): Future[EntitiesMessage] = {
    log.debug("rpc call for listing entities")
    val res = EntityOp.list()

    if (res.isSuccess) {
      Future.successful(EntitiesMessage(Some(AckMessage(AckMessage.Code.OK)), res.get.map(_.toString())))
    } else {
      log.error(res.failed.get)
      Future.successful(EntitiesMessage(Some(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))))
    }
  }


  override def getEntityProperties(request: EntityNameMessage): Future[EntityPropertiesMessage] = {
    log.debug("rpc call for returning entity properties")
    val res = EntityOp.properties(request.entity)

    if (res.isSuccess) {
      Future.successful(EntityPropertiesMessage(Some(AckMessage(AckMessage.Code.OK)), request.entity, res.get))
    } else {
      log.error(res.failed.get)
      Future.successful(EntityPropertiesMessage(Some(AckMessage(AckMessage.Code.ERROR, res.failed.get.getMessage))))
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
      case RepartitionMessage.PartitionOptions.CREATE_NEW => PartitionMode.CREATE_NEW
      case RepartitionMessage.PartitionOptions.CREATE_TEMP => PartitionMode.CREATE_TEMP
      case RepartitionMessage.PartitionOptions.REPLACE_EXISTING => PartitionMode.REPLACE_EXISTING
      case _ => PartitionMode.CREATE_NEW
    }

    val res = IndexOp.partition(request.index, request.numberOfPartitions, None, cols, option)

    if (res.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK, res.get.indexname))
    } else {
      log.error(res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = res.failed.get.getMessage))
    }
  }

  override def setIndexWeight(request: IndexWeightMessage): Future[AckMessage] = {
    log.debug("rpc call for changing weight of index")
    val res = IndexOp.setWeight(request.index, request.weight)

    if (res.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK, request.index))
    } else {
      log.error(res.failed.get)
      Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = "please re-try"))
    }
  }

  override def generateAllIndexes(request: IndexMessage): Future[AckMessage] = {
    val res = IndexOp.generateAll(request.entity, request.column, RPCHelperMethods.prepareDistance(request.distance.get))

    if (res.isSuccess) {
      Future.successful(AckMessage(AckMessage.Code.OK))
    } else {
      log.error(res.failed.get)
      Future.successful(AckMessage(AckMessage.Code.ERROR))
    }
  }
}