package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.EntityNameHolder
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

  @deprecated
  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int)(implicit ac: AdamContext): DataFrame = throw new UnsupportedOperationException

  /** Returns the partitions to be queried for a given Featurevector */
  override def getPartitions(q: FeatureVector, dropPercentage: Double, indexName: EntityNameHolder)(implicit ac: AdamContext): Seq[Int] = throw new UnsupportedOperationException
}
