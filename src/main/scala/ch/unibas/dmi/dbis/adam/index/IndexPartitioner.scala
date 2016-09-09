package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.helpers.partition.{PartitionMode, PartitionerChoice, RandomPartitioner, SparkPartitioner}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.{DataFrame, SaveMode}

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
    * @param cols        columns to partition on, if not specified the primary key is used
    * @param mode        partition mode
    * @param partitioner Which Partitioner you want to use.
    * @param options     Options for partitioner. See each partitioner for details
    * @return
    */
  def apply(index: Index, nPartitions: Int, join: Option[DataFrame], cols: Option[Seq[String]], mode: PartitionMode.Value, partitioner: PartitionerChoice.Value = PartitionerChoice.SPARK, options: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): Try[Index] = {
    log.debug("Repartitioning Index: " + index.indexname + " with partitioner " + partitioner)
    var data = index.getData().get.join(index.entity.get.getData().get, index.pk.name)

    //TODO: possibly consider replication
    //http://stackoverflow.com/questions/31624622/is-there-a-way-to-change-the-replication-factor-of-rdds-in-spark
    //data.persist(StorageLevel.MEMORY_ONLY_2) new StorageLevel(...., N)

    if (join.isDefined) {
      data = data.join(join.get, index.pk.name)
    }

    //lazy because df.repartition() doesn't need it
    lazy val toPartition = {
      if (cols.isDefined) data.map(r => (r.getAs[Any](cols.get.head), r)) else data.map(r => (r.getAs[Any](index.pk.name), r))
    }

    data = partitioner match {
      case PartitionerChoice.SPARK =>
        new SparkPartitioner(nPartitions).repartition(data, cols, Some(index))
      case PartitionerChoice.RANDOM =>
        ac.sqlContext.createDataFrame(toPartition.partitionBy(new RandomPartitioner(nPartitions)).map(_._2), data.schema)
      case PartitionerChoice.CURRENT => {
        ac.sqlContext.createDataFrame(toPartition.partitionBy(new HashPartitioner(nPartitions)).map(_._2), data.schema)
      }
    }

    data = data.select(index.pk.name, FieldNames.featureIndexColumnName)

    mode match {
      case PartitionMode.CREATE_NEW =>
        val newName = Index.createIndexName(index.entityname, index.attribute, index.indextypename)
        CatalogOperator.createIndex(newName, index.entityname, index.attribute, index.indextypename, index.metadata.get)
        Index.storage.create(newName, Seq()) //TODO: switch index to be an entity with specific fields
        val status = Index.storage.write(newName, data)

        if (status.isFailure) {
          throw status.failed.get
        }

        IndexLRUCache.invalidate(newName)

        Success(Index.load(newName).get)

      case PartitionMode.CREATE_TEMP =>
        val newName = Index.createIndexName(index.entityname, index.attribute, index.indextypename)

        val newIndex = index.shallowCopy(Some(newName))
        newIndex.setData(data)

        IndexLRUCache.put(newName, newIndex)
        Success(newIndex)

      case PartitionMode.REPLACE_EXISTING =>
        val status = Index.storage.write(index.indexname, data, SaveMode.Overwrite)

        if (status.isFailure) {
          throw status.failed.get
        }

        IndexLRUCache.invalidate(index.indexname)

        Success(index)

      case _ => Failure(new GeneralAdamException("partitioning mode unknown"))
    }
  }
}
