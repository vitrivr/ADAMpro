package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.EntityNameHolder
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.spark.sql.DataFrame

/**
  * adampar
  *
  * Created by silvan on 20.06.16.
  */
abstract class ADAMPartitioner{

  /** Which partitioner this is */
  def partitionerName: PartitionerChoice.Value

  /**
    * Maybe in the future the indexname will be removed and each partitioner will train on their own keys.
    *
    * @param data DataFrame you want to partition
    * @param cols Columns you want to perform the partition on. If none are provided, the index pk is used instead
    * @param indexName Will be used to store partitioner information in the catalog
    * @param nPartitions how many partitions shall be created
    * @return the partitioned DataFrame
    */
  def apply(data: DataFrame, cols: Option[Seq[String]] = None, indexName: Option[EntityNameHolder] = None, nPartitions: Int)(implicit ac: AdamContext): DataFrame

  /** Returns the partitions to be queried for a given Featurevector */
  def getPartitions(q: FeatureVector, dropPercentage: Double, indexName: EntityNameHolder)(implicit ac: AdamContext): Seq[Int]
}
