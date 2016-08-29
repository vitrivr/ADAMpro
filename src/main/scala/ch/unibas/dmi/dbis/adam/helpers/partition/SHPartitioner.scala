package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.{Entity, EntityNameHolder}
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.sh.{SHIndexMetaData, SHUtils}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.random.Sampling

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by silvanheller on 26.07.16.
  */
class SHPartitioner(meta: SHPartitionerMetaData) extends Partitioner with Logging {
  override def numPartitions: Int = meta.getNoPartitions

  /** We just expect the key to be a bitstring */
  override def getPartition(key: Any): Int = {
    val bitString = key.asInstanceOf[BitString[_]]
    meta.getClusters.zipWithIndex.sortBy(_._1.hammingDistance(bitString)).head._2
  }
}

/**
  * Maybe it's useful to analyze the distribution of the data for training
  * See i.e. Hubness http://perun.pmf.uns.ac.rs/radovanovic/publications/2011-pakdd-khubs.pdf
  * Currently uses random leaders again. K-means in euclidian space failed.
  */
object SHPartitioner extends ADAMPartitioner with Logging with Serializable {

  /** Returns the partitions to be queried for a given Featurevector */
  override def getPartitions(q: FeatureVector, dropPercentage: Double, indexName: EntityNameHolder)(implicit ac: AdamContext): Seq[Int] = {
    val bitstring = getBitString(q, indexName)
    val meta = CatalogOperator.getPartitionerMeta(indexName).get.asInstanceOf[SHPartitionerMetaData]
    val sorted = meta.getClusters.zipWithIndex.sortBy(_._1.hammingDistance(bitstring))
    sorted.dropRight((sorted.size*dropPercentage).toInt).map(_._2)
  }

  override def partitionerName = PartitionerChoice.SH

  /** Sample 400 points and select the point with the biggest distance */
  def trainClusters(joinDF: DataFrame, nPart: Int, noBits: Int)(implicit ac: AdamContext): IndexedSeq[BitString[_]] = {
    //TODO Magic Number
    val trainingsize = 400
    val n = joinDF.count
    val fraction = Sampling.computeFractionForSampleSize(trainingsize, n, false)

    //Choose one random leader initally
    var leaders = ListBuffer[BitString[_]](BitString(Seq.tabulate(noBits)( el => if(Random.nextBoolean()) el else 0).filter(_!=0)))

    def getMinDistance(c: BitString[_]) : Int = leaders.sortBy(_.hammingDistance(c)).last.hammingDistance(c)

    while(leaders.size<nPart){
      var trainData: Array[BitString[_]] = joinDF.sample(false, fraction).collect().map(_.getAs[BitString[_]](FieldNames.partitionKey))
      if(trainData.length<100) trainData = trainData ++ joinDF.take(100).map(_.getAs[BitString[_]](FieldNames.partitionKey))
      leaders+=trainData.sortBy(r => getMinDistance(r)).last
      if(leaders.size%20==0) log.debug(leaders.size.toString)
    }
    leaders.toIndexedSeq
  }


  /**
    * Uses the train()-Function to train 'leaders' in the hamming space. Tuples will be assigned to leaders and then assigned Partitions based on their leaders.
    *
    * @param data DataFrame you want to partition
    * @param cols Ignored here
    * @param indexName Will be used to store partitioner information in the catalog
    * @param nPartitions how many partitions shall be created
    * @return the partitioned DataFrame
    */
  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int, options:Map[String, String] = Map[String, String]())(implicit ac: AdamContext): DataFrame = {
    if (indexName.isEmpty) {
      throw new GeneralAdamException("Indexname was not specified")
    }
    //This line causes you to load the data from the first index that is found which matches the type
    val index = Entity.load(Index.load(indexName.get).get.entityname).get.indexes.find(f => f.get.indextypename == IndexTypes.SHINDEX).get.get
    val noBits = CatalogOperator.getIndexMeta(index.indexname).get.asInstanceOf[SHIndexMetaData].noBits
    val joinDF = index.getData.withColumnRenamed(FieldNames.featureIndexColumnName, FieldNames.partitionKey)
    val joinedDF = data.join(joinDF, index.pk.name)

    val clusters = trainClusters(joinDF, nPartitions, noBits)
    val meta = new SHPartitionerMetaData(nPartitions, noBits, clusters)

    //drop old partitioner, create new
    CatalogOperator.dropPartitioner(indexName.get).get
    CatalogOperator.createPartitioner(indexName.get, nPartitions, meta, SHPartitioner)

    //repartition
    val partitioner = new SHPartitioner(meta)
    val repartitioned: RDD[(Any, Row)] = joinedDF.map(r => (r.getAs[Any](FieldNames.partitionKey), r)).partitionBy(partitioner)
    val reparRDD = repartitioned.mapPartitions((it) => {
      it.map(f => f._2)
    }, preservesPartitioning = true)

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
class SHPartitionerMetaData(nPart: Int, noBits: Int, clusters: IndexedSeq[BitString[_]]) extends Serializable {
  def getNoPartitions: Int = nPart

  def getNoBits: Int = noBits

  def getClusters: IndexedSeq[BitString[_]] = clusters
}