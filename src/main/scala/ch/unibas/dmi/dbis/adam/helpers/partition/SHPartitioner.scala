package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.entity.{Entity, EntityNameHolder}
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by silvanheller on 26.07.16.
  */
class SHPartitioner(nPart: Int) extends Partitioner with ADAMPartitioner with Logging {
  override def numPartitions: Int = nPart

  //TODO Hardcoded
  val noBits = 20

  val floor = 0
  val ceiling = Math.pow(2, noBits.toDouble) - 1
  //TODO Maybe use mod here
  val gap = Math.ceil((ceiling - floor) / nPart.toDouble).toInt

  log.info("Number of Partitions: " + nPart)
  log.info("Number of Bits: " + noBits)
  log.info("Ceiling: " + ceiling)
  log.info("Gap between partitions: " + gap)

  /**
    * We expect the key here to a bitstring.
    *
    * Careful: BitString is stored as an  array of Indices where the bit is set to true.
    *
    * @param key
    * @return
    */
  override def getPartition(key: Any): Int = {
    val bitString = key.asInstanceOf[BitString[_]]
    //TODO Rounding behavior...
    //TODO Partition by hamming distance and not by number...
    val string = bitString.toByteArray.mkString(",")
    //log.info("Getting partition for bitstring: "+string)
    val bits: Seq[Int] = bitString.getBitIndexes
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

  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int)(implicit ac: AdamContext): DataFrame = {
    val indextype = IndexTypes.SHINDEX
    if(indexName.isEmpty){
      throw new GeneralAdamException("Indexname was not specified")
    }
    try {
      //This line causes you to load the data from the first index that is found
      val joinDF = Entity.load(Index.load(indexName.get).get.entityname).get.indexes.find(f => f.get.indextypename == indextype).get.get.getData.withColumnRenamed(FieldNames.featureIndexColumnName, FieldNames.partitionKey)

      val joinedDF = data.join(joinDF, FieldNames.pk)
      log.debug("joinedDF")
      joinedDF.show(2)
      log.debug(joinedDF.first().getAs(FieldNames.partitionKey).getClass+" | "+joinedDF.first().getAs(FieldNames.partitionKey).toString)
      val test = joinDF.map(f => f)
      test.first().mkString(", ")
      val mapped: RDD[(BitString[_], Row)] = joinedDF.map(f => (f.getAs[BitString[_]](FieldNames.partitionKey), f))
      log.debug(mapped.first().toString())

      val repartitioned: RDD[(Any, Row)] = joinedDF.map(r => (r.getAs[Any](FieldNames.partitionKey), r)).partitionBy(new SHPartitioner(nPartitions))

      log.debug("Sampled from repartitioned: "+repartitioned.first()._2.mkString(", ") +repartitioned.first()._2.getAs[BitString[_]](1).getBitIndexes.mkString(","))
      val reparRDD = repartitioned.mapPartitions((it) => {
        it.map(f => f._2)
      })
      log.debug("Sampled from return RDD: "+reparRDD.first().mkString(", ") +" | "+reparRDD.first().getAs[BitString[_]](1).getBitIndexes.mkString(","))
      val res = ac.sqlContext.createDataFrame(reparRDD, joinedDF.schema)
      res.show(2)
      val end = res.select(FieldNames.pk, FieldNames.featureIndexColumnName)
      end.show(2)
      res
    } catch {
      case e: java.util.NoSuchElementException => {
        log.error("Repartitioning with this mode is not possible because the index: " + indextype.name + " does not exist", e)
        throw new GeneralAdamException("Index: " + indextype.name + " does not exist, aborting repartitioning")
      }
    }
  }
}