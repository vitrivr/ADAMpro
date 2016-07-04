package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.handler.FlatFileHandler
import ch.unibas.dmi.dbis.adam.storage.partition.PartitionMode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object EntityPartitioner {
  /**
    *
    * @param entity entity
    * @param nPartitions number of partitions
    * @param join join with dataframe for partitioning
    * @param cols columns according to which to partition the data
    * @param mode mode of partitioning (replacing data, etc.)
    * @return
    */
  def apply(entity: Entity, nPartitions: Int, join: Option[DataFrame], cols: Option[Seq[String]], mode: PartitionMode.Value)(implicit ac: AdamContext): Try[Entity] = {
    //checks
    if (entity.featureData.isEmpty) {
      return Failure(new GeneralAdamException("no feature data available for performing repartitioning"))
    }

    val partitionAttributes = entity.schema(Some(cols.getOrElse(Seq(entity.pk.name))))

    if (!partitionAttributes.filterNot(_.pk).forall(_.storagehandler.isDefined)) {
      return Failure(new GeneralAdamException("repartitioning is only possible for defined handlers"))
    }

    if (!partitionAttributes.filterNot(_.pk).forall(_.storagehandler.get.isInstanceOf[FlatFileHandler])) {
      return Failure(new GeneralAdamException("repartitioning is only possible using the flat file handler"))
    }

    if (partitionAttributes.filterNot(_.pk).map(_.storagehandler).distinct.length > 1) {
      return Failure(new GeneralAdamException("repartitioning is only possible for data within one handler"))
    }

    //collect data
    var data = entity.data().get
    if (join.isDefined) {
      data = data.join(join.get, entity.pk.name)
    }

    //repartition
    //TODO: possibly add own partitioner
    //data.map(r => (r.getAs[Any](cols.get.head), r)).partitionBy(new HashPartitioner())
    data = if (cols.isDefined) {
      val entityColNames = data.schema.map(_.name)
      if (!cols.get.forall(name => entityColNames.contains(name))) {
        Failure(throw new GeneralAdamException("one of the columns " + cols.mkString(",") + " is not existing in entity " + entity.entityname + entityColNames.mkString("(", ",", ")")))
      }

      data.repartition(nPartitions, cols.get.map(data(_)): _*)
    } else {
      data.repartition(nPartitions, data(entity.pk.name))
    }

    val handler = partitionAttributes.filterNot(_.pk).headOption
      .getOrElse(entity.schema().filter(_.storagehandler.isDefined).filter(_.storagehandler.get.isInstanceOf[FlatFileHandler]).head)
          .storagehandler.get

    //select data which is available in the one handler
    val attributes = entity.schema().filterNot(_.pk).filter(_.storagehandler.get == handler).+:(entity.pk)
    data = data.select(attributes.map(attribute => col(attribute.name)).toArray : _*)

    mode match {
      case PartitionMode.REPLACE_EXISTING =>
        val status = handler.write(entity.entityname, data, SaveMode.Overwrite)

        if (status.isFailure) {
          throw status.failed.get
        }

        entity.markStale()
        EntityLRUCache.invalidate(entity.entityname)

        Success(entity)

      case _ => Failure(new GeneralAdamException("partitioning mode unknown"))
    }
  }
}
