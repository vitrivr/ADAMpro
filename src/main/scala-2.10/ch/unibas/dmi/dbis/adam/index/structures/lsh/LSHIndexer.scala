package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.{EuclideanHashFunction, Hasher}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.ADAMSamplingUtils


class LSHIndexer(hashFamily : String, numHashTables : Int, numHashes : Int, distance : DistanceFunction, trainingSize : Int) extends IndexGenerator with Serializable {
  override val indextypename: IndexTypeName = IndexStructures.LSH

  /**
   *
   * @param indexname
   * @param entityname
   * @param data
   * @return
   */
  override def index(indexname : IndexName, entityname : EntityName, data: RDD[IndexingTaskTuple]): Index[_ <: IndexTuple] = {
    val n = Entity.countTuples(entityname)
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(trainingSize, n, false)
    val trainData = data.sample(false, fraction)

    val indexMetaData = train(trainData.collect())

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
   * @param trainData
   * @return
   */
  private def train(trainData : Array[IndexingTaskTuple]) : LSHIndexMetaData = {
    //data
    val radiuses = {
        val res = for (a <- trainData; b <- trainData) yield distance(a.value, b.value)
        if(res.isEmpty){
          Seq().iterator
        }  else {
          Seq(res.max).iterator
        }
      }
    val radius = radiuses.sum / radiuses.length

    //TODO: hashFamily move to apply; use currying?
    val dims = trainData.head.value.size

    val hashFamily = () => new EuclideanHashFunction(dims, radius.toFloat, 256)

    val hashTables = (0 until numHashTables).map(i => new Hasher(hashFamily, numHashes))

    LSHIndexMetaData(hashTables, radius.toFloat)
  }
}


object LSHIndexer {
  /**
   *
   * @param properties
   */
  def apply(distance : DistanceFunction, properties : Map[String, String] = Map[String, String]()) : IndexGenerator = {
    val hashFamilyDescription = properties.getOrElse("hashFamily", "euclidean")
    val hashFamily = hashFamilyDescription.toLowerCase match {
      case "euclidean" => "euclidean" //TODO: check how to pass params
      case _ => null
    }

    val numHashTables = properties.getOrElse("numHashTables", "64").toInt
    val numHashes = properties.getOrElse("numHashes", "64").toInt

    val norm = properties.getOrElse("norm", "2").toInt

    val trainingSize = properties.getOrElse("trainingSize", "500").toInt

    new LSHIndexer(hashFamily, numHashTables, numHashes, distance, trainingSize)
  }
}
