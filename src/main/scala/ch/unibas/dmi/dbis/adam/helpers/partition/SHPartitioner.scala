package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.entity.EntityNameHolder
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.Partitioner
import org.apache.spark.sql.DataFrame

/**
  * Created by silvanheller on 26.07.16.
  */
class SHPartitioner(nPart: Int) extends Partitioner with ADAMPartitioner with Logging {
  override def numPartitions: Int = nPart

  override def getPartition(key: Any): Int = {
    ???
  }

  override def partitionerName = PartitionerChoice.SH
  /**
    *
    * @param data        DataFrame you want to partition
    * @param cols        Columns you want to perform the partition on. If none are provided, the index pk is used instead
    * @param indexName   If this is index-data, you can specify the name of the index here. Will be used to determine pk.
    *                    If no indexName is provided, we just partition by the head of the dataframe-schema
    * @param nPartitions how many partitions shall be created
    * @return the partitioned DataFrame
    */
  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int): DataFrame = throw new UnsupportedOperationException
}
