package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.entity.{Entity, EntityNameHolder}
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.DataFrame

/**
  * adampar
  *
  * Created by silvan on 20.06.16.
  */
trait ADAMPartitioner{

  def partitionerName: PartitionerChoice.Value

  /**
    *
    * @param data DataFrame you want to partition
    * @param cols Columns you want to perform the partition on. If none are provided, the index pk is used instead
    * @param indexName If this is index-data, you can specify the name of the index here. Will be used to determine pk.
    *                  If no indexName is provided, we just partition by the head of the dataframe-schema
    * @param nPartitions how many partitions shall be created
    * @return the partitioned DataFrame
    */
  def apply(data: DataFrame, cols: Option[Seq[String]] = None, indexName: Option[EntityNameHolder] = None, nPartitions: Int)  : DataFrame
}
