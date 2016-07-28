package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.helpers.partition._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

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
    var data = index.data.join(index.entity.get.getData().get, index.pk.name)

    //TODO: possibly consider replication
    //http://stackoverflow.com/questions/31624622/is-there-a-way-to-change-the-replication-factor-of-rdds-in-spark
    //data.persist(StorageLevel.MEMORY_ONLY_2) new StorageLevel(...., N)

    if (join.isDefined) {
      data = data.join(join.get, index.pk.name)
    }

    //lazy because df.repartition() doesn't need it
    //Extracts an rdd (key, value) where value is the rdd-row and key is either cols.head or the pk
    lazy val toPartition: RDD[(Any, Row)] = {
      if (cols.isDefined) data.map(r => (r.getAs[Any](cols.get.head), r)) else data.map(r => (r.getAs[Any](index.pk.name), r))
    }

    //repartition
    data = partitioner match {
      case PartitionerChoice.SPARK =>
        SparkPartitioner(data, cols, Some(index.indexname), nPartitions)
      case PartitionerChoice.RANDOM =>
        ac.sqlContext.createDataFrame(toPartition.partitionBy(new RandomPartitioner(nPartitions)).map(_._2), data.schema)
      case PartitionerChoice.CURRENT => {
        val indextype = IndexTypes.SHINDEX
        try{
          log.debug("\n --------------- \n Partitioning to "+nPartitions+" with Current Implementation")
          //TODO Switch from Info log. Extract code below to SHPartitioner
          val originSchema = data.schema
          //log.debug("Original schema: \n "+originSchema.treeString)

          log.info("Sampled BitString from the original DF "+data.head().getAs[BitString[_]](FieldNames.featureIndexColumnName).getBitIndexes.mkString(", "))

          //TODO The direct access to the data param here is also not pretty
          val joinDF = Entity.load(index.entityname).get.indexes.find(f => f.get.indextypename == indextype).get.get.data.withColumnRenamed(FieldNames.featureIndexColumnName, FieldNames.partitionKey)
          //log.info("Join Schema: \n "+joinDF.schema.treeString)
          //log.info("Sampled BitString from the joinDF: "+joinDF.head().getAs[BitString[_]](FieldNames.partitionKey).getBitIndexes.mkString(", "))

          data = data.join(joinDF, index.pk.name)
          //log.info("Joined Schema: \n " +data.schema.treeString)
          //log.info("Sampled BitString from the joined df: oriringal feature - "+data.head().getAs[BitString[_]](FieldNames.featureIndexColumnName).getBitIndexes.mkString(", ")+" | partKey - "+data.head().getAs[BitString[_]](FieldNames.partitionKey).getBitIndexes.mkString(", "))

          //TODO Currently this is hardcoded now since we can't get bitlength from the index
          val repartitioned: RDD[(Any, Row)] = data.map(r => (r.getAs[Any](FieldNames.partitionKey), r)).partitionBy(new SHPartitioner(nPartitions, 20))

          val reparRDD: RDD[Row] = repartitioned.mapPartitions((it) => {it.map(f => Row(f._2.getAs(index.pk.name), f._2.getAs(FieldNames.featureIndexColumnName)))})
          val retDF = ac.sqlContext.createDataFrame(reparRDD, originSchema)
          //log.info("New DF schema: \n"+retDF.schema.treeString)
          //log.info("Sampled BitString from the repartitioned DF "+retDF.head().getAs[BitString[_]](FieldNames.featureIndexColumnName).getBitIndexes.mkString(", ")+" | data partKey - "+data.head().getAs[BitString[_]](FieldNames.partitionKey).getBitIndexes.mkString(", ") + " | joinDF partKey - " + joinDF.head().getAs[BitString[_]](FieldNames.partitionKey).getBitIndexes.mkString(", ") )
          retDF
        }catch{
          case e: java.util.NoSuchElementException => {
            log.error("Repartitioning with this mode is not possible because the index: "+indextype.name + " does not exist", e)
            throw new GeneralAdamException("Index: "+indextype.name+" does not exist, aborting repartitioning")
          }
        }
      }
      case PartitionerChoice.RANGE =>
        {
          //TODO This needs implicit ordering?
          throw new UnsupportedOperationException
          //ac.sqlContext.createDataFrame(toPartition.partitionBy(new RangePartitioner[(Any, Row)](nPartitions, toPartition)))
        }
    }
    data = data.select(index.pk.name, FieldNames.featureIndexColumnName)
    log.debug("New Data Schema: "+data.schema.treeString)
    log.debug("Sampled Row: "+data.head.toString() + "| "+data.head.getAs[BitString[_]](FieldNames.featureIndexColumnName).getBitIndexes.mkString(", "))

    mode match {
      case PartitionMode.CREATE_NEW =>
        val newName = Index.createIndexName(index.entityname, index.attribute, index.indextypename)
        CatalogOperator.createIndex(newName, index.entityname, index.attribute, index.indextypename, index.metadata)
        Index.storage.create(newName, Seq()) //TODO: switch index to be an entity with specific fields
        val status = Index.storage.write(newName, data)

        if (status.isFailure) {
          throw status.failed.get
        }
        IndexLRUCache.invalidate(newName)
        Success(Index.load(newName).get)

      case PartitionMode.CREATE_TEMP =>
        val newName = Index.createIndexName(index.entityname, index.attribute, index.indextypename)

        val newIndex = index.copy(Some(newName))
        newIndex.data = data

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
