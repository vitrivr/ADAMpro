package org.vitrivr.adampro.index.partition

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.datatypes.vector.Vector.MathVector
import org.vitrivr.adampro.entity.EntityNameHolder
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.index.Index.IndexName
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.utils.Logging

import scala.util.Random

/**
  * ADAMpar.
  *
  * Uses Spark-Default Partitioning which is based on Hash Partitioning or Round Robin.
  * Uses Hash Partitioning if you specify a column and round robin if no column is specified
  *
  * Silvan Heller
  * June 2016
  */
object SparkPartitioner extends CustomPartitioner with Logging with Serializable {
  override def partitionerName = PartitionerChoice.SPARK

  /**
    * @param data        DataFrame you want to partition
    * @param attribute   Columns you want to perform the partition on. If none are provided, the index pk is used instead
    * @param indexName   Will be used to store partitioner information in the catalog
    * @param nPartitions how many partitions shall be created
    * @return the partitioned DataFrame
    */
  override def apply(data: DataFrame, attribute: Option[String], indexName: Option[IndexName], nPartitions: Int, options: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): DataFrame = {
    SparkStartup.catalogOperator.dropPartitioner(indexName.get)
    SparkStartup.catalogOperator.createPartitioner(indexName.get, nPartitions, null, SparkPartitioner)

    val partCol = if (attribute.isDefined) {
      val entityColNames = data.schema.map(_.name)

      if (!entityColNames.contains(attribute.get)) {
        throw new GeneralAdamException("the columns " + attribute + " does not exist in the data " + entityColNames.mkString("(", ",", ")"))
      }

      attribute.get
    } else {
      if (indexName.isDefined) {
        Index.load(indexName.get).get.pk.name
      } else {
        data.schema.head.name
      }
    }

    val rdd = data.rdd.map(r => (r.getAs[Any](partCol), r)).partitionBy(new HashPartitioner(nPartitions)).map(_._2)
    ac.sqlContext.createDataFrame(rdd, data.schema)
  }

  /**
    * Drops just random partitions except the one where a hash partitioner would put the FeatureVector
    */
  override def getPartitions(q: MathVector, dropPercentage: Double, indexName: EntityNameHolder)(implicit ac: AdamContext): Seq[Int] = {
    val noPart = SparkStartup.catalogOperator.getNumberOfPartitions(indexName).get
    val part = new HashPartitioner(noPart).getPartition(q)
    val parts = Random.shuffle(Seq.tabulate(noPart)(el => el)).filter(_ != part)
    parts.dropRight((noPart * dropPercentage).toInt) ++ Seq(part)
  }
}
