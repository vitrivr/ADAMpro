package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.EntityNameHolder
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.{Failure, Random}

/**
  * adampar
  *
  * Uses Spark-Default Partitioning which is based on Hash Partitioning
  *
  * Created by silvan on 20.06.16.
  */
object SparkPartitioner extends ADAMPartitioner with Logging with Serializable{
  override def partitionerName = PartitionerChoice.SPARK

  /**
    * Uses sparks built-in Hash-Partitioner
    *
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

  //TODO How do we enable a 'fair' comparison of this partitioner? Since it's based on a hashpartitioner we only know which partition this specific key will be assigned to.
  /**
    * Drops just random partitions except the one where the hashpartitioner would put the FeatureVector atm
    */
  override def getPartitions(q: FeatureVector, dropPercentage: Double, indexName: EntityNameHolder)(implicit ac: AdamContext): Seq[Int] = {
    val noPart = CatalogOperator.getNumberOfPartitions(indexName).get
    val part = new HashPartitioner(noPart).getPartition(q)
    val parts = Random.shuffle(Seq.tabulate(noPart)(el => el)).filter(_!=part)
    parts.dropRight((noPart*dropPercentage).toInt) ++ Seq(part)
  }
}
