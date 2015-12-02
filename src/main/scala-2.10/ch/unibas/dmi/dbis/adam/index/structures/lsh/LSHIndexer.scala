package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.{EuclideanHashFunction, Hasher}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.spark.rdd.RDD


class LSHIndexer(hashFamily : String, numHashTables : Int, numHashes : Int, distance : DistanceFunction, trainingSize : Int) extends IndexGenerator with Serializable {
  override val indextypename: IndexTypeName = IndexStructures.LSH

  /**
   *
   * @param indexname
   * @param entityname
   * @param data
   * @return
   */
  override def index(indexname : IndexName, entityname : EntityName, data: RDD[IndexerTuple]): Index[_ <: IndexTuple] = {
    val indexMetaData = train(data)

    val indexdata = data.map(
      datum => {
        val hash = LSHUtils.hashFeature(datum.value, indexMetaData)
        BitStringIndexTuple(datum.tid, hash)
      })

    import SparkStartup.sqlContext.implicits._
    new LSHIndex(indexname, entityname, indexdata.toDF, indexMetaData)
  }

  /**
   *
   * @param data
   * @return
   */
  private def train(data : RDD[IndexerTuple]) : LSHIndexMetaData = {
    //data
    val n = data.countApprox(5000).getFinalValue().mean.toInt

    val trainData = data.sample(false, math.min(trainingSize, n) / n)

    val radius = trainData.mapPartitions { iter =>
        val seq = iter.toSeq
        val res = for (a <- seq; b <- seq) yield distance(a.value, b.value)
        res.iterator
      }.mean()

    //TODO: hashFamily move to apply; use currying?
    val dims = data.first.value.length
    val hashFamily = () => EuclideanHashFunction(dims, radius.toFloat, 256)

    val hashTables = (0 until numHashTables).map(i => new Hasher(hashFamily, numHashes))

    LSHIndexMetaData(hashTables, radius.toFloat)
  }
}


object LSHIndexer {
  /**
   *
   * @param properties
   */
  def apply(properties : Map[String, String] = Map[String, String](), distance : DistanceFunction, data: RDD[IndexerTuple]) : IndexGenerator = {
    val hashFamilyDescription = properties.getOrElse("hashFamily", "euclidean")
    val hashFamily = hashFamilyDescription.toLowerCase match {
      case "euclidean" => "euclidean" //TODO: check how to pass params
      case _ => null
    }

    val numHashTables = properties.getOrElse("numHashTables", "64").toInt
    val numHashes = properties.getOrElse("numHashes", "64").toInt

    val norm = properties.getOrElse("norm", "2").toInt

    val trainingSize = properties.getOrElse("trainingSize", "5000").toInt

    new LSHIndexer(hashFamily, numHashTables, numHashes, distance, trainingSize)
  }
}
