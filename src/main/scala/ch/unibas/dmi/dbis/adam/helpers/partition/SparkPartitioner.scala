package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.util.Failure

/**
  * adampar
  *
  * Created by silvan on 20.06.16.
  */
class SparkPartitioner(nPart: Int) extends Partitioner with ADAMPartitioner with Logging with Serializable{
  override def partitionerName = PartitionerChoice.SPARK


  val hashPart = new HashPartitioner(nPart)

  /**
    * If no col is specified, partitions by the pk or the first col of the DataFrame
    */
  def repartition(data: DataFrame, cols: Option[Seq[String]] = None, index: Option[Index] = None): DataFrame = {
    if (cols.isDefined) {
      val entityColNames = data.schema.map(_.name)
      if (!cols.get.forall(name => entityColNames.contains(name))) {
        Failure(throw new GeneralAdamException("one of the columns " + cols.mkString(",") + " does not exist in the data " + entityColNames.mkString("(", ",", ")")))
      }

      data.repartition(nPart, cols.get.map(col): _*)
    } else {
      if (index.isDefined) {
        data.repartition(nPart, data(index.get.pk.name))
      } else {
        data.repartition(nPart, data(data.schema.head.name))
      }
    }
  }

  @Override
  def getPartition(key: Any): Int = {
    hashPart.getPartition(key)
  }

  override def numPartitions: Int = nPart
}
