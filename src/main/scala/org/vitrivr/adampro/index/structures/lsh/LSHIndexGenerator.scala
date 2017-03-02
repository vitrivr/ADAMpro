package org.vitrivr.adampro.index.structures.lsh

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.datatypes.vector.Vector
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.index.Index.IndexTypeName
import org.vitrivr.adampro.index._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.index.structures.lsh.hashfunction.{EuclideanHashFunction, Hasher, ManhattanHashFunction}
import org.vitrivr.adampro.index.structures.lsh.signature.LSHSignatureGenerator
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.{DistanceFunction, EuclideanDistance, ManhattanDistance}


class LSHIndexGenerator(numHashTables: Int, numHashes: Int, m: Int, distance: DistanceFunction, trainingSize: Int)(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.LSHINDEX

  /**
    *
    * @param data raw data to index
    * @return
    */
  override def index(data: DataFrame, attribute : String)(tracker : OperationTracker): (DataFrame, Serializable) = {
    log.trace("LSH started indexing")

    val meta = train(getSample(math.max(trainingSize, MINIMUM_NUMBER_OF_TUPLE), attribute)(data))
    val signatureGenerator = new LSHSignatureGenerator(meta.hashTables, meta.m)

    val signatureUDF = udf((c: DenseSparkVector) => {
      signatureGenerator.toSignature(Vector.conv_dspark2vec(c)).serialize
    })
    val indexed = data.withColumn(AttributeNames.featureIndexColumnName, signatureUDF(data(attribute)))

    log.trace("LSH finished indexing")

    (indexed, meta)
  }

  /**
    *
    * @param trainData training data
    * @return
    */
  private def train(trainData: Seq[IndexingTaskTuple]): LSHIndexMetaData = {
    log.trace("LSH started training")

    //data
    val dims = trainData.head.ap_indexable.size


    //compute average radius for query
    val radiuses = {
      val res = for (a <- trainData; b <- trainData) yield (a.ap_id, distance(a.ap_indexable, b.ap_indexable))
      res.groupBy(_._1).map(x => x._2.map(_._2).max)
    }.toSeq
    val radius = radiuses.sum / radiuses.length

    val hashFamily = distance match {
      case ManhattanDistance => () => new ManhattanHashFunction(dims, radius.toFloat, m)
      case EuclideanDistance => () => new EuclideanHashFunction(dims, radius.toFloat, m)
      case _ => null
    }
    val hashTables = (0 until numHashTables).map(i => new Hasher(hashFamily, numHashes))

    log.trace("LSH finished training")

    LSHIndexMetaData(hashTables.toArray, radius.toFloat, distance, m)
  }
}


class LSHIndexGeneratorFactory extends IndexGeneratorFactory {
  /**
    * @param distance   distance function
    * @param properties indexing properties
    */
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator = {
    val numHashTables = properties.getOrElse("nhashtables", "64").toInt
    val numHashes = properties.getOrElse("nhashes", "64").toInt
    val maxBuckets = properties.getOrElse("nbuckets", "256").toInt

    val norm = properties.getOrElse("norm", "2").toInt

    val trainingSize = properties.getOrElse("ntraining", "500").toInt

    new LSHIndexGenerator(numHashTables, numHashes, maxBuckets, distance, trainingSize)
  }

  /**
    *
    * @return
    */
  override def parametersInfo: Seq[ParameterInfo] = Seq(
    new ParameterInfo("ntraining", "number of training tuples", Seq[String]()),
    new ParameterInfo("nhashtables", "number of hash tables (are OR-ed)", Seq(16, 32, 64, 128, 256).map(_.toString)),
    new ParameterInfo("nhashes", "number of hashes (are AND-ed)", Seq(16, 32, 64, 128, 256).map(_.toString)),
    new ParameterInfo("nbuckets", "maximum number of buckets per hash table", Seq(16, 32, 64, 128, 256).map(_.toString)),
    new ParameterInfo("norm", "norm to use (defines hash function)", Seq[String]())
  )
}
