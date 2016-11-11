package org.vitrivr.adampro.index.structures.lsh

import org.vitrivr.adampro.config.FieldNames
import org.vitrivr.adampro.datatypes.bitString.BitStringUDT
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity._
import org.vitrivr.adampro.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.index._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.index.structures.lsh.hashfunction.{EuclideanHashFunction, Hasher, ManhattanHashFunction}
import org.vitrivr.adampro.index.structures.lsh.signature.LSHSignatureGenerator
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.{DistanceFunction, EuclideanDistance, ManhattanDistance}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.random.Sampling


class LSHIndexGenerator(numHashTables: Int, numHashes: Int, m: Int, distance: DistanceFunction, trainingSize: Int)(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.LSHINDEX

  /**
    *
    * @param indexname  name of index
    * @param entityname name of entity
    * @param data       data to index
    * @return
    */
  override def index(indexname: IndexName, entityname: EntityName, data: RDD[IndexingTaskTuple[_]]): (DataFrame, Serializable) = {
    val entity = Entity.load(entityname).get

    val n = entity.count
    val fraction = Sampling.computeFractionForSampleSize(math.max(trainingSize, MINIMUM_NUMBER_OF_TUPLE), n, withReplacement = false)
    var trainData = data.sample(false, fraction).collect()
    if (trainData.length < MINIMUM_NUMBER_OF_TUPLE) {
      trainData = data.take(MINIMUM_NUMBER_OF_TUPLE)
    }

    val meta = train(trainData)

    log.debug("LSH indexing...")

    val signatureGenerator = new LSHSignatureGenerator(meta.hashTables, meta.m)

    val indexdata = data.map(
      datum => {
        //compute hash for each vector
        val hash = signatureGenerator.toSignature(datum.feature)
        Row(datum.id, hash)
      })

    val schema = StructType(Seq(
      StructField(entity.pk.name, entity.pk.fieldtype.datatype, nullable = false),
      StructField(FieldNames.featureIndexColumnName, new BitStringUDT, nullable = false)
    ))

    val df = ac.sqlContext.createDataFrame(indexdata, schema)

    (df, meta)
  }

  /**
    *
    * @param trainData training data
    * @return
    */
  private def train(trainData: Array[IndexingTaskTuple[_]]): LSHIndexMetaData = {
    log.trace("LSH started training")

    //data
    val dims = trainData.head.feature.size


    //compute average radius for query
    val radiuses = {
      val res = for (a <- trainData; b <- trainData) yield (a.id, distance(a.feature, b.feature))
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
