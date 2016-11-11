package org.vitrivr.adampro.helpers.partition

import org.vitrivr.adampro.catalog.CatalogOperator
import org.vitrivr.adampro.datatypes.feature.Feature.FeatureVector
import org.vitrivr.adampro.entity.EntityNameHolder
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.utils.Logging
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.{Failure, Random}

/**
  * ADAMpar.
  *
  * Uses Spark-Default Partitioning which is based on Hash Partitioning or Round Robin.
  * Uses Hash Partitioning if you specify a column and round robin if no column is specified
  *
  * Silvan Heller
  * June 2016
  */
object SparkPartitioner extends CustomPartitioner with Logging with Serializable{

  override def partitionerName = PartitionerChoice.SPARK

  /**
    * @param data DataFrame you want to partition
    * @param cols Columns you want to perform the partition on. If none are provided, the index pk is used instead
    * @param indexName Will be used to store partitioner information in the catalog
    * @param nPartitions how many partitions shall be created
    * @return the partitioned DataFrame
    */
  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int, options:Map[String, String] = Map[String, String]())(implicit ac: AdamContext): DataFrame = {
    CatalogOperator.dropPartitioner(indexName.get)
    CatalogOperator.createPartitioner(indexName.get,nPartitions,null,SparkPartitioner)

    if (cols.isDefined) {
      val entityColNames = data.schema.map(_.name)
      if (!cols.get.forall(name => entityColNames.contains(name))) {
        Failure(throw new GeneralAdamException("one of the columns " + cols.mkString(",") + " does not exist in the data " + entityColNames.mkString("(", ",", ")")))
      }
      data.repartition(nPartitions, cols.get.map(col): _*)
    } else {
      if (indexName.isDefined) {
        val index = Index.load(indexName.get)
        data.repartition(nPartitions, data(index.get.pk.name))
      } else {
        data.repartition(nPartitions, data(data.schema.head.name))
      }
    }
  }
  /**
    * Drops just random partitions except the one where a hash partitioner would put the FeatureVector
    */
  override def getPartitions(q: FeatureVector, dropPercentage: Double, indexName: EntityNameHolder)(implicit ac: AdamContext): Seq[Int] = {
    val noPart = CatalogOperator.getNumberOfPartitions(indexName).get
    val part = new HashPartitioner(noPart).getPartition(q)
    val parts = Random.shuffle(Seq.tabulate(noPart)(el => el)).filter(_!=part)
    parts.dropRight((noPart*dropPercentage).toInt) ++ Seq(part)
  }
}
