package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.EntityNameHolder
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.Partitioner
import org.apache.spark.sql.DataFrame

import scala.util.Random

/**
  * Created by silvan on 21.06.16.
  */
class RandomPartitioner(nPart: Int) extends Partitioner with Logging with Serializable{
  override def numPartitions: Int = nPart

  /**
    * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
    *
    * @return
    */
  override def getPartition(key: Any): Int = {
    (Random.nextFloat()*nPart).toInt
  }
}

object RandomPartitioner extends ADAMPartitioner{
  override def partitionerName = PartitionerChoice.RANDOM

  /**
    * Throws each key in a random partition
    * @param data DataFrame you want to partition
    * @param cols Does not matter in this mode
    * @param indexName will be used to store the partitioner in the Catalog
    * @param nPartitions how many partitions shall be created
    * @return the partitioned DataFrame
    */
  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int, options:Map[String, String] = Map[String, String]())(implicit ac: AdamContext): DataFrame = {
    val schema = data.schema
    CatalogOperator.dropPartitioner(indexName.get)
    CatalogOperator.createPartitioner(indexName.get,nPartitions,null,RandomPartitioner)
    val toPartition = if (cols.isDefined) data.map(r => (r.getAs[Any](cols.get.head), r)) else data.map(r => (r.getAs[Any](Index.load(indexName.get).get.pk.name), r))
    ac.sqlContext.createDataFrame(toPartition.partitionBy(new RandomPartitioner(nPartitions)).mapPartitions(r => r.map(_._2), true), schema)
  }

  /** Returns the partitions to be queried for a given Featurevector
    * Returns Random Partitions
    * */
  override def getPartitions(q: FeatureVector, dropPercentage: Double, indexName: EntityNameHolder)(implicit ac: AdamContext): Seq[Int] = {
    val nPart = CatalogOperator.getNumberOfPartitions(indexName).get
    Random.shuffle(Seq.tabulate(nPart)(el => el)).drop((nPart*dropPercentage).toInt)
  }
}
