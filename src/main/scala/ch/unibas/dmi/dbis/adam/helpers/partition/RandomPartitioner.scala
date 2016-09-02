package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.Partitioner

import scala.util.Random

/**
  * Created by silvan on 21.06.16.
  */
class RandomPartitioner(nPart: Int) extends Partitioner with CustomPartitioner with Logging {
  override def numPartitions: Int = nPart

  /**
    * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
    * @return
    */
  override def getPartition(key: Any): Int = {
    (Random.nextFloat()*nPart).toInt
  }

  override def partitionerName = PartitionerChoice.RANDOM
}
