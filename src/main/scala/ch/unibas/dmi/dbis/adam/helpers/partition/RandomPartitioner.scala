package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.entity.EntityNameHolder
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.Partitioner
import org.apache.spark.sql.DataFrame

import scala.util.Random

/**
  * Created by silvan on 21.06.16.
  */
class RandomPartitioner(nPart: Int) extends Partitioner with ADAMPartitioner with Logging with Serializable{
  override def numPartitions: Int = nPart

  /**
    * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
    *
    * @return
    */
  override def getPartition(key: Any): Int = {
    (Random.nextFloat()*nPart).toInt
  }

  override def partitionerName = PartitionerChoice.RANDOM

  @deprecated
  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int): DataFrame = throw new UnsupportedOperationException
}
