package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.EntityNameHolder
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.spark.sql.DataFrame

/**
  * adampar
  *
  * TODO Currently the partitioners have a lot of code which looks similar. Maybe in a separation-of concerns sense it would be good to have more methods here
  * which call abstract methods. i.e. the Repartitioning-Code looks very similar and dropping the old and inserting the new Partitioner in the catalog is not something
  * every Partitioner should have to think of
  *
  * TODO Also, currently one partitioner is stored per index in the Catalog. Maybe it would be smarter to store partitioners independently
  * But then, you'd have to give them names, pks etc.
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
  def apply(data: DataFrame, cols: Option[Seq[String]] = None, indexName: Option[EntityNameHolder] = None, nPartitions: Int, options : Map[String, String] = Map[String, String]())(implicit ac: AdamContext): DataFrame

  /** Returns the partitions to be queried for a given Featurevector */
  def getPartitions(q: FeatureVector, dropPercentage: Double, indexName: EntityNameHolder)(implicit ac: AdamContext): Seq[Int]
}
