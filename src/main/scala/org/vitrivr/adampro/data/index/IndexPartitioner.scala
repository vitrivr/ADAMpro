package org.vitrivr.adampro.data.index

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.vitrivr.adampro.shared.catalog.CatalogManager
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.data.entity.Entity.AttributeName
import org.vitrivr.adampro.utils.exception.GeneralAdamException
import org.vitrivr.adampro.data.index.partition._
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.utils.Logging

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
object IndexPartitioner extends Logging {
  /**
    * Partitions the index data.
    *
    * @param index       index
    * @param nPartitions number of partitions
    * @param join        other dataframes to join on, on which the partitioning is performed
    * @param attribute        columns to partition on, if not specified the primary key is used
    * @param mode        partition mode
    * @param partitioner Which Partitioner you want to use.
    * @param options     Options for partitioner. See each partitioner for details
    * @return
    */
  def apply(index: Index, nPartitions: Int, join: Option[DataFrame], attribute: Option[AttributeName], mode: PartitionMode.Value, partitioner: PartitionerChoice.Value = PartitionerChoice.SPARK, options: Map[String, String] = Map[String, String]())(implicit ac: SharedComponentContext): Try[Index] = {
    log.debug("Repartitioning Index: " + index.indexname + " with partitioner " + partitioner)
    var data = index.getData().get.join(index.entity.get.getData().get, index.pk.name)

    //TODO: possibly consider replication
    //http://stackoverflow.com/questions/31624622/is-there-a-way-to-change-the-replication-factor-of-rdds-in-spark
    //data.persist(StorageLevel.MEMORY_ONLY_2) new StorageLevel(...., N)

    if (join.isDefined) {
      data = data.join(join.get, index.pk.name)
    }

    try {
      //repartition
      data = partitioner match {
        case PartitionerChoice.SPARK => SparkPartitioner(data, attribute, Some(index.indexname), nPartitions)
        case PartitionerChoice.RANDOM => RandomPartitioner(data, attribute, Some(index.indexname), nPartitions)
        case PartitionerChoice.ECP => ECPPartitioner(data, attribute, Some(index.indexname), nPartitions)
      }

      data = data.select(index.pk.name, AttributeNames.featureIndexColumnName)
    } catch {
      case e: Exception => return Failure(e)
    }

    mode match {
      case PartitionMode.CREATE_NEW =>
        val newName = Index.createIndexName(index.entityname, index.attribute, index.indextypename)
        ac.catalogManager.createIndex(newName, index.entityname, index.attribute, index.indextypename, index.metadata.get)
        Index.getStorage().get.create(newName, Seq()) //TODO: switch index to be an entity with specific fields
        val status = Index.getStorage().get.write(newName, data, Seq())

        if (status.isFailure) {
          throw status.failed.get
        }

        ac.cacheManager.invalidateIndex(newName)

        Success(Index.load(newName).get)

      case PartitionMode.CREATE_TEMP =>
        val newName = Index.createIndexName(index.entityname, index.attribute, index.indextypename)

        val newIndex = index.shallowCopy(Some(newName))
        newIndex.setData(data)

        ac.cacheManager.put(newName, newIndex)
        Success(newIndex)

      case PartitionMode.REPLACE_EXISTING =>
        val status = Index.getStorage().get.write(index.indexname, data, Seq(), SaveMode.Overwrite)

        if (status.isFailure) {
          throw status.failed.get
        }

        ac.cacheManager.invalidateIndex(index.indexname)

        Success(index)

      case _ => Failure(new GeneralAdamException("partitioning mode unknown"))
    }
  }
}
