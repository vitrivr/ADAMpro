package ch.unibas.dmi.dbis.adam.helpers.partition

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.{Entity, EntityNameHolder}
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.sh.{SHIndexMetaData, SHUtils}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.Partitioner
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.random.Sampling

/**
  * Created by silvanheller on 26.07.16.
  */
class SHPartitioner(meta: SHPartitionerMetaData) extends Partitioner with Logging {
  override def numPartitions: Int = meta.getNoPartitions

  /** We just expect the key to be a bitstring */
  override def getPartition(key: Any): Int = {
    val bitString = key.asInstanceOf[BitString[_]]
    meta.getClusters.predict(Vectors.dense(SHPartitioner.toVector(bitString, meta.getNoBits).map(_.toDouble).toArray))
  }
}

/**
  * Maybe it's useful to analyze the distribution of the data for training
  * See i.e. Hubness http://perun.pmf.uns.ac.rs/radovanovic/publications/2011-pakdd-khubs.pdf
  * Currently uses simple K-Means. The spark k-means transforms the hamming space into a euclidian one. This needs to be evaluated
  * Maybe in the future this could train an SH-Hashfunction on the training data / use the PQ-Idea / use eCP.
  */
object SHPartitioner extends ADAMPartitioner with Logging with Serializable {

  /** Returns the partitions to be queried for a given Featurevector */
  override def getPartitions(q: FeatureVector, dropPercentage: Double, indexName: EntityNameHolder)(implicit ac: AdamContext): Seq[Int] = {
    val bitstring = getBitString(q, indexName)
    val meta = CatalogOperator.getPartitionerMeta(indexName).get.asInstanceOf[SHPartitionerMetaData]

    //This is really ugly code
    def vectovec(f : Vector ) : FeatureVector= {
      Feature.conv_stored2vector(Feature.conv_doublestored2floatstored(f.toArray))
    }
    val sorted = meta.getClusters.clusterCenters.zipWithIndex.sortBy(el => EuclideanDistance(q, vectovec(el._1)))
    sorted.dropRight((sorted.size*dropPercentage).toInt).map(_._2).toSeq
  }

  override def partitionerName = PartitionerChoice.SH

  /** Trains a K-Means Model */
  def trainClusters(joinDF: DataFrame, nPart: Int, noBits: Int)(implicit ac: AdamContext): KMeansModel = {
    //TODO Magic Number
    val trainingsize = 1000
    val n = joinDF.count
    val fraction = Sampling.computeFractionForSampleSize(trainingsize, n, false)
    var trainData: Array[Row] = joinDF.sample(false, fraction).collect()
    if (trainData.length < trainingsize) {
      trainData = joinDF.take(trainingsize - trainData.length)
    }
    val ex = trainData.head.getAs[BitString[_]](FieldNames.partitionKey)
    val vectors = ac.sc.parallelize(trainData.map(f => Vectors.dense(toVector(f.getAs[BitString[_]](FieldNames.partitionKey), noBits).map(f => f.toDouble).toArray)))
    KMeans.train(vectors, nPart, 100)
  }

  /** Transforms a BitString to a Vector with 0 and 1. Needs to be here and not in the bs-interface because the bs doesn't know how long he could be */
  def toVector(bs: BitString[_], noBits: Int) : scala.collection.immutable.Vector[Double] = {
    Seq.tabulate(noBits)(el => if(bs.getBitIndexes.contains(el)) 1.0 else 0.0).toVector
  }

  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int)(implicit ac: AdamContext): DataFrame = {
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
class SHPartitionerMetaData(nPart: Int, noBits: Int, clusters: KMeansModel) extends Serializable {
  def getNoPartitions: Int = nPart

  def getNoBits: Int = noBits

  def getClusters: KMeansModel = clusters
}