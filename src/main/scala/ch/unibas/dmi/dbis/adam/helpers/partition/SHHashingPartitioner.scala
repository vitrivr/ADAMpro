package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.{BitString, EWAHBitString}
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.{Entity, EntityNameHolder}
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.sh.{SHIndexMetaData, SHIndexer, SHUtils}
import ch.unibas.dmi.dbis.adam.index.{Index, IndexingTaskTuple}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import com.googlecode.javaewah.datastructure.BitSet
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.random.Sampling

import scala.collection.mutable.ListBuffer

/**
  * Created by silvanheller on 26.07.16.
  */
class SHHashingPartitioner(meta: SHHashingPartitionerMetaData) extends Partitioner with Logging {
  override def numPartitions: Int = meta.getNoPartitions

  /** We just expect the key to be a bitstring */
  override def getPartition(key: Any): Int = {
    val bitString = key.asInstanceOf[BitString[_]]
    val hash = SHUtils.hashFeature(SHHashingPartitioner.toVector(bitString, meta.getNoBits), meta.getMeta)
    var part = 0
    while(hash.iterator.hasNext){
      part+=Math.pow(2,hash.iterator.next).toInt
    }
    part
  }
}


object SHHashingPartitioner extends ADAMPartitioner with Logging with Serializable {

  /** Returns the partitions to be queried for a given Featurevector */
  override def getPartitions(q: FeatureVector, dropPercentage: Double, indexName: EntityNameHolder)(implicit ac: AdamContext): Seq[Int] = {
    val bitstring = getBitString(q, indexName)
    val meta = CatalogOperator.getPartitionerMeta(indexName).get.asInstanceOf[SHHashingPartitionerMetaData]
    val leaders = Seq.tabulate(meta.getNoPartitions)(el => EWAHBitString(convert(el)))
    val sorted = leaders.zipWithIndex.sortBy(_._1.hammingDistance(bitstring))
    sorted.dropRight((sorted.size*dropPercentage).toInt).map(_._2)
  }

  /** Converts Long to BitSet*/
  def convert(value: Long) : BitSet = {
    val bits = new BitSet()
    var index = 0
    var modVal = value
    while (modVal != 0L) {
      if (modVal % 2L != 0) {
        bits.set(index)
      }
      index+=1
      modVal = modVal>>> 1
    }
    bits
  }

  override def partitionerName = PartitionerChoice.CURRENT

  def train(joinDF: DataFrame, nPart: Int, noBits: Int)(implicit ac: AdamContext): SHHashingPartitionerMetaData = {
    //TODO Magic Number
    val trainingsize = 1001
    val n = joinDF.count
    val fraction = Sampling.computeFractionForSampleSize(trainingsize, n, false)
    var trainData: Array[IndexingTaskTuple[_]] = joinDF.sample(false, fraction).collect().map(f => IndexingTaskTuple(0, toVector(f.getAs[BitString[_]](FieldNames.partitionKey), noBits)))

    if(trainData.length < trainingsize){
      trainData = joinDF.take(trainingsize).map(f => IndexingTaskTuple(0,toVector(f.getAs[BitString[_]](FieldNames.partitionKey), noBits)))
    }

    val indexmeta = SHIndexer.train(trainData, Some(noBits))

    new SHHashingPartitionerMetaData(nPart, noBits, indexmeta)
  }

  def toVector(bs: BitString[_], noBits: Int) : FeatureVector = {
    val lb = ListBuffer[Double]()
    var nxt = if(bs.iterator.hasNext) bs.iterator.next() else -1
    var idx = 0
    while(bs.iterator.hasNext && idx < noBits){
      if(nxt==idx){
        lb+=1d
        nxt = bs.iterator.next()
      } else lb+=0d
      idx+=1
    }
    Feature.conv_stored2vector(Feature.conv_doublestored2floatstored(lb.toVector))
  }

  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int, options:Map[String, String] = Map[String, String]())(implicit ac: AdamContext): DataFrame = {
    return data
    //TODO Train is broken atm
    if (indexName.isEmpty) {
      throw new GeneralAdamException("Indexname was not specified")
    }
    //This line causes you to load the data from the first index that is found which matches the type
    val index = Entity.load(Index.load(indexName.get).get.entityname).get.indexes.find(f => f.get.indextypename == IndexTypes.SHINDEX).get.get
    val noBits = CatalogOperator.getIndexMeta(index.indexname).get.asInstanceOf[SHIndexMetaData].noBits
    val joinDF = index.getData.withColumnRenamed(FieldNames.featureIndexColumnName, FieldNames.partitionKey)
    val joinedDF = data.join(joinDF, index.pk.name)

    val meta = train(joinDF, nPartitions, noBits)

    //drop old partitioner, create new
    CatalogOperator.dropPartitioner(indexName.get).get
    CatalogOperator.createPartitioner(indexName.get, nPartitions, meta, SHHashingPartitioner)

    //repartition
    val partitioner = new SHHashingPartitioner(meta)
    val repartitioned: RDD[(Any, Row)] = joinedDF.map(r => (r.getAs[Any](FieldNames.partitionKey), r)).partitionBy(partitioner)
    val reparRDD = repartitioned.mapPartitions((it) => {
      it.map(f => f._2)
    }, true)

    ac.sqlContext.createDataFrame(reparRDD, joinedDF.schema)
  }

  /**
    * Loads the Information for the Partitioner and Index corresponding to the given entity and then returns the BitString
    * TODO Maybe make this more independent from the entity
    * @param q     Queryvector
    * @param indexname The entity belonging to the Index should have an SH-Index
    * @return BitString hashed by the SHUtils
    */
  def getBitString(q: FeatureVector, indexname: EntityNameHolder)(implicit ac: AdamContext): BitString[_] = {
    val index = Entity.load(Index.load(indexname).get.entityname).get.indexes.find(f => f.get.indextypename == IndexTypes.SHINDEX).get.get
    val metaData = CatalogOperator.getIndexMeta(index.indexname).get.asInstanceOf[SHIndexMetaData]
    SHUtils.hashFeature(q, metaData)
  }
}

/** Metadata Class to store Information */
class SHHashingPartitionerMetaData(nPart: Int, noBits: Int, indexmeta: SHIndexMetaData) extends Serializable {
  def getNoPartitions: Int = nPart

  def getNoBits: Int = noBits

  def getMeta : SHIndexMetaData = indexmeta
}