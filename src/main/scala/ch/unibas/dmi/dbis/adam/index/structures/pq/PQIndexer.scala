package ch.unibas.dmi.dbis.adam.index.structures.pq

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, ByteType, StructField, StructType}
import org.apache.spark.util.random.Sampling

import scala.collection.immutable.IndexedSeq

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class PQIndexer(nsq: Int, trainingSize: Int)(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override def indextypename: IndexTypeName = IndexTypes.PQINDEX

  override def index(indexname: IndexName, entityname: EntityName, data: RDD[IndexingTaskTuple[_]]): Index = {
    val entity = Entity.load(entityname).get

    val n = entity.count
    val fraction = Sampling.computeFractionForSampleSize(math.max(trainingSize, IndexGenerator.MINIMUM_NUMBER_OF_TUPLE), n, false)
    var trainData = data.sample(false, fraction).collect()
    if(trainData.length < IndexGenerator.MINIMUM_NUMBER_OF_TUPLE){
      trainData = data.take(IndexGenerator.MINIMUM_NUMBER_OF_TUPLE)
    }

    val indexMetaData = train(trainData)

    val d = trainData.head.feature.size

    log.debug("PQ indexing...")

    val indexdata = data.map(
      datum => {
        val hash = datum.feature.toArray
          .grouped(math.max(1, d / nsq)).toSeq
          .zipWithIndex
          .map { case (split, idx) => indexMetaData.models(idx).predict(Vectors.dense(split.map(_.toDouble))).toByte }
        Row(datum.id, hash)
      })


    val schema = StructType(Seq(
      StructField(entity.pk.name, entity.pk.fieldtype.datatype, false),
      StructField(FieldNames.featureIndexColumnName, new ArrayType(ByteType, false), false)
    ))

    val df = ac.sqlContext.createDataFrame(indexdata, schema)
    new PQIndex(indexname, entityname, df, indexMetaData)
  }

  /**
    *
    * @param trainData training data
    * @return
    */
  private def train(trainData: Array[IndexingTaskTuple[_]]): PQIndexMetaData = {
    log.trace("PQ started training")

    val numIterations = 100
    val nsqbits: Int = 8 //index produces a byte array index tuple
    val numClusters: Int = 2 ^ nsqbits

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

    log.trace("PQ finished training")

    PQIndexMetaData(clusters, nsq)
  }
}

object PQIndexer {
  /**
    * @param distance   distance function
    * @param properties indexing properties
    */
  def apply(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator = {
    val nsq = properties.getOrElse("nsq", "8").toInt
    val trainingSize = properties.getOrElse("ntraining", "500").toInt

    new PQIndexer(nsq, trainingSize)
  }
}