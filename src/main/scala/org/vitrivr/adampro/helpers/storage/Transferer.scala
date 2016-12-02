package org.vitrivr.adampro.helpers.storage

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.SaveMode
import org.vitrivr.adampro.catalog.CatalogOperator
import org.vitrivr.adampro.config.AdamConfig
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.utils.Logging

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * December 2016
  */
object Transferer extends Logging {

  /**
    *
    * @param entity
    * @param attributes
    * @param newHandlerName
    */
  @Experimental def apply(entity: Entity, attributes: Seq[String], newHandlerName: String)(implicit ac: AdamContext): Try[Void] = {
    try {
      val schema = entity.schema()

      assert(attributes.forall(schema.map(_.name).contains(_)))

      assert(ac.storageHandlerRegistry.value.get(newHandlerName).isDefined)
      val storagehandler = ac.storageHandlerRegistry.value.get(newHandlerName).get

      val fieldtypes = schema.filter(x => attributes.contains(x.name)).map(_.fieldtype).distinct
      assert(fieldtypes.forall(storagehandler.supports.contains(_)))

      //data transfer

      //all attributes that should be transfered + the ones that are already in place in this storagehandler + pk
      val dataAttributes = attributes ++ (schema.filter(_.storagehandlername == newHandlerName).map(_.name)).+:(entity.pk.name)
      val data = entity.getData(Some(dataAttributes.distinct)).get.repartition(AdamConfig.defaultNumberOfPartitions)

      storagehandler.write(entity.entityname, data, schema.filter(dataAttributes.contains(_)), SaveMode.Overwrite)

      schema.filterNot(x => dataAttributes.contains(x.name)).groupBy(_.storagehandlername).filterNot(_._1 == newHandlerName).foreach { case (handlername, attributes) =>
        val fields = if (!attributes.exists(_.name == entity.pk.name)) {
          attributes.+:(entity.pk)
        } else {
          attributes
        }

        val df = entity.getData(Some(attributes.map(_.name))).get
        //TODO: check here for PK!
        val handler = ac.storageHandlerRegistry.value.get(newHandlerName).get
        val status = handler.write(entity.entityname, df, fields, SaveMode.Append, Map("allowRepartitioning" -> "true", "partitioningKey" -> entity.pk.name))

        if (status.isFailure) {
          throw status.failed.get
        }
      }

      if (schema.filter(_.storagehandlername == newHandlerName).nonEmpty) {
        //i.e. the old file was overwritten anyways
      } else {
        //i.e. the old file has to be deleted or adjusted (if other data is still contained in there)
        //TODO...
      }

      //adjust attribute storage handler
      attributes.foreach { attribute =>
        CatalogOperator.updateAttributeStorageHandler(entity.entityname, attribute, newHandlerName)
      }

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}
