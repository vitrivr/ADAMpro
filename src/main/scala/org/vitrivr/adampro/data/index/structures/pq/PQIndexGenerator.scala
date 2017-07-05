package org.vitrivr.adampro.data.index.structures.pq

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.utils.exception.{GeneralAdamException, QueryNotConformException}
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.data.index.Index.IndexTypeName
import org.vitrivr.adampro.data.index._
import org.vitrivr.adampro.data.index.structures.IndexTypes
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.distance.{DistanceFunction, HammingDistance, MinkowskiDistance}

import scala.collection.immutable.IndexedSeq

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class PQIndexGenerator(nsq: Int, trainingSize: Int)(@transient implicit val ac: SharedComponentContext) extends IndexGenerator {
  override def indextypename: IndexTypeName = IndexTypes.PQINDEX

  /**
    *
    * @param data raw data to index
    * @return
    */
  override def index(data: DataFrame, attribute : String)(tracker : QueryTracker): (DataFrame, Serializable) = {
    log.trace("PQ started indexing")

    val sample = getSample(math.max(trainingSize, MINIMUM_NUMBER_OF_TUPLE), attribute)(data)
    val meta = train(sample)
    val dim = sample.head.ap_indexable.size

    assert(dim >= nsq)

    val cellUDF = udf((c: DenseSparkVector) => {
      c.grouped(math.max(1, dim / nsq)).toSeq
        .zipWithIndex
        .map { case (split, idx) => meta.models(idx).predict(Vectors.dense(split.map(_.toDouble).toArray)).toByte }
    })
    val indexed = data.withColumn(AttributeNames.featureIndexColumnName, cellUDF(data(attribute)))

    log.trace("PQ finished indexing")

    (indexed, meta)
  }

  /**
    *
    * @param trainData training data
    * @return
    */
  private def train(trainData: Seq[IndexingTaskTuple]): PQIndexMetaData = {
    log.trace("PQ started training")

    val numIterations = 100
    val nsqbits: Int = 8 //index produces a byte array index tuple
    val numClusters: Int = 2 ^ nsqbits

    val d = trainData.head.ap_indexable.size

    //split vectors in sub-vectors and assign to part
    val rdds = trainData.map(_.ap_indexable).flatMap(t =>
      t.toArray.grouped(math.max(1, d / nsq)).toSeq.zipWithIndex)
      .groupBy(_._2)
      .mapValues(vs => vs.map(_._1))
      .mapValues(vs => vs.map(v => Vectors.dense(v.map(_.toDouble))))
      .mapValues(vs => ac.sc.parallelize(vs))
      .toIndexedSeq
      .sortBy(_._1)
      .map(_._2)


    //cluster single parts
    val clusters: IndexedSeq[KMeansModel] = rdds.map { rdd =>
      KMeans.train(rdd, numClusters, numIterations)
    }

    log.trace("PQ finished training")

    PQIndexMetaData(clusters, nsq)
  }
}

class PQIndexGeneratorFactory extends IndexGeneratorFactory {
  /**
    * @param distance   distance function
    * @param properties indexing properties
    */
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: SharedComponentContext): IndexGenerator = {
    if(!distance.isInstanceOf[MinkowskiDistance]){
      throw new QueryNotConformException("PQ index only supports Manhattan distance")
    }

    val nsq = properties.getOrElse("nsq", "8").toInt
    val trainingSize = properties.getOrElse("ntraining", "1000").toInt

    new PQIndexGenerator(nsq, trainingSize)
  }

  /**
    *
    * @return
    */
  override def parametersInfo: Seq[ParameterInfo] = Seq(
    new ParameterInfo("ntraining", "number of training tuples", Seq[String]()),
    new ParameterInfo("nsq", "number of sub-vectors", Seq(4, 8, 16, 32, 64, 128, 256, 512, 1024).map(_.toString))
  )
}