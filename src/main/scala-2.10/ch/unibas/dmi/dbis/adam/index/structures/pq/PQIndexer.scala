package ch.unibas.dmi.dbis.adam.index.structures.pq

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.log4j.Logger
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.ADAMSamplingUtils

import scala.collection.immutable.IndexedSeq

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class PQIndexer(nsq: Int, trainingSize: Int)(@transient implicit val ac : AdamContext) extends IndexGenerator with Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  override def indextypename : IndexTypeName = IndexTypes.PQINDEX

  override def index(indexname: IndexName, entityname: EntityName, data: RDD[IndexingTaskTuple]): Index = {
    val n = EntityHandler.countTuples(entityname).get
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(trainingSize, n, false)
    val trainData = data.sample(false, fraction)
    val collected = trainData.collect()
    val indexMetaData = train(collected)

    val d = collected.head.feature.size


    log.debug("PQ indexing...")

    val indexdata = data.map(
      datum => {
        val hash = datum.feature.toArray
          .grouped(math.max(1, d / nsq)).toSeq
          .zipWithIndex
          .map{case(split,idx) => indexMetaData.models(idx).predict(Vectors.dense(split.map(_.toDouble))).toByte}
        ByteArrayIndexTuple(datum.id, hash)
      })

    import SparkStartup.Implicits.sqlContext.implicits._
    new PQIndex(indexname, entityname, indexdata.toDF, indexMetaData)
  }

  /**
    *
    * @param trainData
    * @return
    */
  private def train(trainData: Array[IndexingTaskTuple]): PQIndexMetaData = {
    val numIterations = 100
    val nsqbits : Int = 8 //index produces a byte array index tuple
    val numClusters : Int = 2 ^ nsqbits

    val d = trainData.head.feature.size

    val rdds = trainData.map(_.feature).flatMap(t =>
      t.toArray.grouped(math.max(1, d / nsq)).toSeq.zipWithIndex)
      .groupBy(_._2)
      .mapValues(vs => vs.map(_._1))
      .mapValues(vs => vs.map(v => Vectors.dense(v.map(_.toDouble))))
      .mapValues(vs => ac.sc.parallelize(vs))
      .toIndexedSeq
      .sortBy(_._1)
      .map(_._2)


    val clusters: IndexedSeq[KMeansModel] = rdds.map { rdd =>
      KMeans.train(rdd, numClusters, numIterations)
    }

    PQIndexMetaData(clusters, nsq)
  }
}

object PQIndexer {
  /**
    *
    * @param properties
    */
  def apply(distance: DistanceFunction, properties : Map[String, String] = Map[String, String]())(implicit ac : AdamContext) : IndexGenerator = {
    val nsq = properties.getOrElse("nsq", "8").toInt
    val trainingSize = properties.getOrElse("ntraining", "500").toInt

    new PQIndexer(nsq, trainingSize)
  }
}