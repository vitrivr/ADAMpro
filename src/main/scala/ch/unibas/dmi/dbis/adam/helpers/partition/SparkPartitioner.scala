package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.entity.{Entity, EntityNameHolder}
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.{HashPartitioner, Partitioner}
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits._

import scala.util.Failure

/**
  * adampar
  * Uses Spark-Default Partitioning which is based on Hash Partitioning
  *
  * Created by silvan on 20.06.16.
  */
object SparkPartitioner extends ADAMPartitioner with Logging{
  override def partitionerName = PartitionerChoice.SPARK

  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int): DataFrame = {
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
}
