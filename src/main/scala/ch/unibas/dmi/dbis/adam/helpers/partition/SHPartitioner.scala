package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.entity.EntityNameHolder
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
    *
    * Careful: BitString is stored as an array of Indices where the bit is set to true.
    *
    * @param key
    * @return
    */
  override def getPartition(key: Any): Int = {
    val bitString = key.asInstanceOf[BitString[_]]
    //TODO Rounding behavior...
    //log.info("Getting partition for bitstring: "+bitString.toByteArray.mkString(", "))
    val bits: Seq[Int] = bitString.getBitIndexes
    //log.info("Bits set at positions: "+bitString.getBitIndexes.mkString(", "))
    //TODO Is 6 Correct here? It's the static thing in the BitSet that the bitString is supposed to abstract
    //log.info("Integers that are supposedly represented: " + bitString.toInts(noBits,6).mkString(", "))
    var number = 0
    bits.foreach(f => {
      //log.debug("Byte "+f+" is set, "+f)
      number+=Math.pow(2,f).toInt
    })
    //log.info("Bitstring was converted to number: "+number)
    val partition = number/gap
    //log.info("Bitstring was assigned partition: "+partition)
    partition
  }

  override def partitionerName = PartitionerChoice.SH

  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int): DataFrame = throw new UnsupportedOperationException
}
