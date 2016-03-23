package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.api._
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.{FieldDefinition, FieldTypes}
import ch.unibas.dmi.dbis.adam.entity.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.http.grpc.{CreateEntityMessage, AckMessage}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import org.apache.log4j.Logger

import scala.concurrent.Future

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class DataDefinitionRPC extends AdamDefinitionGrpc.AdamDefinition {
  val log = Logger.getLogger(getClass.getName)

  override def createEntity(request: CreateEntityMessage): Future[AckMessage] = {
    log.debug("rpc call for create entity operation")

    try {
      if(!request.fields.isEmpty){
        val entityname = request.entity
        val fields = request.fields.mapValues(field => FieldDefinition(matchFields(field)))
        CreateEntityOp(entityname, Option(fields))
      } else {
        if(request.fields.contains(FieldNames.idColumnName)
          || request.fields.contains(FieldNames.internFeatureColumnName)
          || request.fields.contains(FieldNames.distanceColumnName)){
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
  private def matchFields(ft : CreateEntityMessage.FieldType): FieldType = {
    if(ft.isBoolean) return FieldTypes.BOOLEANTYPE
    if(ft.isDouble) return FieldTypes.DOUBLETYPE
    if(ft.isFloat) return FieldTypes.FLOATTYPE
    if(ft.isInt) return FieldTypes.INTTYPE
    if(ft.isLong) return FieldTypes.LONGTYPE
    if(ft.isString) return FieldTypes.STRINGTYPE

    return FieldTypes.UNRECOGNIZEDTYPE
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


  override def insert(request: InsertMessage): Future[AckMessage] = ???


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

      if(indextypename == null){
        throw new Exception("no index type name given.")
      }


      IndexOp(request.entity, indextypename, NormBasedDistanceFunction(request.norm),  request.options )
      Future.successful(AckMessage(code = AckMessage.Code.OK))
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


  override def dropIndex(request: IndexNameMessage): Future[AckMessage] ={
    log.debug("rpc call for dropping index operation")

    try {
      DropIndexOp(request.entity)
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } catch {
      case e: Exception =>
        log.debug("exception while rpc call for dropping index operation")
        Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))
    }
  }

  override def randomData(request: RandomDataMessage): Future[AckMessage] = {
    log.debug("rpc call for creating random data")

    try {
      RandomDataOp(request.entity, request.ntuples, request.ndims)
      Future.successful(AckMessage(code = AckMessage.Code.OK))
    } catch {
      case e: Exception =>
        log.debug("exception while rpc call for dropping index operation")
        Future.successful(AckMessage(code = AckMessage.Code.ERROR, message = e.getMessage))
    }
  }
}