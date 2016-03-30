package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.{EuclideanHashFunction, Hasher, ManhattanHashFunction}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, EuclideanDistance, ManhattanDistance}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.ADAMSamplingUtils


class LSHIndexer(numHashTables : Int, numHashes : Int, distance : DistanceFunction, trainingSize : Int) extends IndexGenerator with Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  override val indextypename: IndexTypeName = IndexTypes.LSHINDEX

  /**
   *
   * @param indexname
   * @param entityname
   * @param data
   * @return
   */
  override def index(indexname : IndexName, entityname : EntityName, data: RDD[IndexingTaskTuple]): Index = {
    val n = Entity.countTuples(entityname)
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(trainingSize, n, false)
    val trainData = data.sample(false, fraction)

    val indexMetaData = train(trainData.collect())

    log.debug("LSH indexing...")

    val indexdata = data.map(
      datum => {
        val hash = LSHUtils.hashFeature(datum.feature, indexMetaData)
        BitStringIndexTuple(datum.id, hash)
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
    log.debug("LSH started training")

    //data
    val dims = trainData.head.feature.size

    val radiuses = {
        val res = for (a <- trainData; b <- trainData) yield distance(a.feature, b.feature)
        if(res.isEmpty){
          Seq().iterator
        }  else {
          Seq(res.max).iterator
        }
      }
    val radius = radiuses.sum / radiuses.length

    val hashFamily = distance match {
      case ManhattanDistance => () => new ManhattanHashFunction(dims, radius.toFloat, 256)
      case EuclideanDistance => () => new EuclideanHashFunction(dims, radius.toFloat, 256)
      case _ => null
    }
    val hashTables = (0 until numHashTables).map(i => new Hasher(hashFamily, numHashes))

    log.debug("LSH finished training")

    LSHIndexMetaData(hashTables, radius.toFloat, distance)
  }
}


object LSHIndexer {
  /**
   *
   * @param properties
   */
  def apply(distance : DistanceFunction, properties : Map[String, String] = Map[String, String]()) : IndexGenerator = {
    val numHashTables = properties.getOrElse("numHashTables", "64").toInt
    val numHashes = properties.getOrElse("numHashes", "64").toInt

    val norm = properties.getOrElse("norm", "2").toInt

    val trainingSize = properties.getOrElse("trainingSize", "500").toInt

    new LSHIndexer(numHashTables, numHashes, distance, trainingSize)
  }
}
