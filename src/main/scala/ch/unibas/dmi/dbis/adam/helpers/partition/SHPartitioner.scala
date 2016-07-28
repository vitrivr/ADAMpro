package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.entity.EntityNameHolder
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by silvanheller on 26.07.16.
  */
class SHPartitioner(nPart: Int, noBits: Int) extends Partitioner with ADAMPartitioner with Logging {
  override def numPartitions: Int = nPart

  val floor = 0
  val ceiling = Math.pow(2, noBits.toDouble) - 1
  val gap = Math.ceil((ceiling - floor) / nPart.toDouble).toInt

  log.info("Number of Partitions: " + nPart)
  log.info("Number of Bits: " + noBits)
  log.info("Ceiling: " + ceiling)
  log.info("Gap between partitions: " + gap)

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
    val string = bitString.toByteArray.mkString(",")
    //log.info("Getting partition for bitstring: "+string)
    val bits: Seq[Int] = bitString.getBitIndexes
    //log.info("Bits set at positions: "+bitString.getBitIndexes.mkString(", "))
    //Is 6 Correct here? It's the static thing in the BitSet that the bitString is supposed to abstract
    //log.info("Integers that are supposedly represented: " + bitString.toInts(noBits,6).mkString(", "))
    var number = 0
    bits.foreach(f => {
      //log.debug("Byte "+f+" is set, "+f)
      number += Math.pow(2, f).toInt
    })
    //log.info("Bitstring "+string+" was converted to number: "+number)
    val partition = number / gap
    //log.info("Bitstring "+string+" was assigned partition: "+partition)
    partition
  }

  override def partitionerName = PartitionerChoice.SH

  /**
    * Not functional atm
    * @param data DataFrame you want to partition
    * @param cols Columns you want to perform the partition on. If none are provided, the index pk is used instead
    * @param indexName If this is index-data, you can specify the name of the index here. Will be used to determine pk.
    *                  If no indexName is provided, we just partition by the head of the dataframe-schema
    * @param nPartitions how many partitions shall be created
    * @param ac
    * @return the partitioned DataFrame
    */
  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int)(implicit ac: AdamContext): DataFrame = {
    throw new UnsupportedOperationException
    val indextype = IndexTypes.SHINDEX
    if(indexName.isEmpty){
      throw new GeneralAdamException("Indexname was not specified")
    }
    try {
      val originSchema = data.schema
      //TODO We can't load joinDF like that below. But isn't the version with the storage even worse? Since it doesn't use the cache?
      //Index.load(indexName.get).get.storage.read(indexName.get).get
      //val joinDF = Entity.load(Index.load(indexName.get).get.entityname).get.indexes.find(f => f.get.indextypename == indextype).get.get.data.withColumnRenamed(FieldNames.featureIndexColumnName, FieldNames.partitionKey)
      //data = data.join(joinDF, FieldNames.pk)
      //TODO Currently this is hardcoded now since we can't get bitlength from the index. Should be Stored in the Catalog
      val repartitioned: RDD[(Any, Row)] = data.map(r => (r.getAs[Any](FieldNames.partitionKey), r)).partitionBy(new SHPartitioner(nPartitions, 20))
      val reparRDD = repartitioned.mapPartitions((it) => {
        it.map(f => Row(f._2.getAs(FieldNames.pk), f._2.getAs(FieldNames.featureIndexColumnName)))
      })
      ac.sqlContext.createDataFrame(reparRDD, originSchema)
    } catch {
      case e: java.util.NoSuchElementException => {
        log.error("Repartitioning with this mode is not possible because the index: " + indextype.name + " does not exist", e)
        throw new GeneralAdamException("Index: " + indextype.name + " does not exist, aborting repartitioning")
      }
    }
  }
}
