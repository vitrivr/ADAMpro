package org.vitrivr.adampro.entity

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.vitrivr.adampro.exception.{AttributeNotExistingException, GeneralAdamException}
import org.vitrivr.adampro.index.partition.PartitionMode
import org.vitrivr.adampro.main.AdamContext

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object EntityPartitioner {
  /**
    * Partitions the entity data.
    *
    * @param entity      entity
    * @param npartitions number of partitions
    * @param join        join with dataframe for partitioning
    * @param attribute   columns according to which to partition the data
    * @param mode        mode of partitioning (replacing data, etc.)
    * @return
    */
  def apply(entity: Entity, npartitions: Int, join: Option[DataFrame] = None, attribute: Option[String], mode: PartitionMode.Value = PartitionMode.REPLACE_EXISTING)(implicit ac: AdamContext): Try[Entity] = {
    //checks
    if (entity.handlers.filter(_.engine.repartitionable).isEmpty) {
      return Failure(new GeneralAdamException("no partitionable engine available in entity for performing repartitioning"))
    }

    val partAttribute = if(attribute.isDefined){
      entity.schema(nameFilter = Some(Seq(attribute.get))).headOption
    } else {
      Some(entity.pk)
    }

    if (partAttribute.isEmpty) {
      throw new AttributeNotExistingException()
    }

    //collect data
    var data = entity.getData().get
    if (join.isDefined) {
      data = data.join(join.get, entity.pk.name)
    }

    //repartition
    //TODO: use other partitioners
    data = data.repartition(npartitions, col(partAttribute.get.name))

    //store
    mode match {
      case PartitionMode.REPLACE_EXISTING =>
        entity.schema(fullSchema = false).groupBy(_.storagehandler).filter(_._1.engine.repartitionable).foreach {
          case (handler, attributes) =>
            val attributenames = attributes.map(_.name).+:(entity.pk.name)
            val status = handler.write(entity.entityname, data.select(attributenames.map(col): _*), attributes, SaveMode.Overwrite)

            if (status.isFailure) {
              throw status.failed.get
            }
        }

        entity.markStale()

        Success(entity)
      case _ => Failure(new GeneralAdamException("partitioning mode unknown"))
    }
  }

}
