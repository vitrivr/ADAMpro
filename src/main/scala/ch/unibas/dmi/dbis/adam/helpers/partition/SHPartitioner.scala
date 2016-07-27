package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.entity.EntityNameHolder
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.Partitioner
import org.apache.spark.sql.DataFrame

/**
  * Created by silvanheller on 26.07.16.
  */
class SHPartitioner(nPart: Int, noBits: Int) extends Partitioner with ADAMPartitioner with Logging {
  override def numPartitions: Int = nPart
  val floor = 0

  val ceiling = Math.pow(2, noBits.toDouble)-1

  val gap = Math.ceil((ceiling-floor)/nPart.toDouble).toInt


  log.info("Number of Partitions: "+nPart)
  log.info("Number of Bits: "+noBits)
  log.info("Ceiling: "+ceiling)
  log.info("Gap between partitions: "+gap)

  /**
    * We expect the key here to a bitstring.
    * @param key
    * @return
    */
  override def getPartition(key: Any): Int = {
    val bitString = key.asInstanceOf[BitString[_]]
    //TODO Rounding behavior...
    log.info("Getting partition for bitstring: "+bitString.toByteArray.mkString)
    val intKey = bitString.toInts(1, bitString.toByteArray.length)(0)
    log.info("Bitstring was converted to Integer: "+intKey)
    val partition = intKey/gap
    log.info("Bitstring was assigned partition: "+partition)
    partition
  }

  override def partitionerName = PartitionerChoice.SH

  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int): DataFrame = throw new UnsupportedOperationException
}
