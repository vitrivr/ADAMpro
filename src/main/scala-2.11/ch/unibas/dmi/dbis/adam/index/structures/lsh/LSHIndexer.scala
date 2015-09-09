package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.datatypes.Feature
import Feature.WorkingVector
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.{EuclideanHashFunction, Hasher, LSHashFunction}
import ch.unibas.dmi.dbis.adam.index.{IndexerTuple, Index, IndexGenerator, IndexTuple}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.rdd.RDD


class LSHIndexer(hashFamily : () => LSHashFunction, numHashTables : Int, numHashes : Int, distance : NormBasedDistanceFunction, trainingSize : Int) extends IndexGenerator with Serializable {
  override val indextypename : String = "lsh"

  /**
   *
   * @param indexname
   * @param tablename
   * @param data
   * @return
   */
  override def index(indexname : IndexName, tablename : TableName, data: RDD[IndexerTuple[WorkingVector]]): Index = {
    val indexMetaData = train(data)

    val indexdata = data.map(
      datum => {
        val hash = LSHUtils.hashFeature(datum.value, indexMetaData)
        IndexTuple(datum.tid, hash)
      })

    import SparkStartup.sqlContext.implicits._
    new LSHIndex(indexname, tablename, indexdata.toDF, indexMetaData)
  }

  /**
   *
   * @param data
   * @return
   */
  private def train(data : RDD[IndexerTuple[WorkingVector]]) : LSHIndexMetaData = {
    //data
    val trainData = data.takeSample(true, trainingSize)

    val hashTables = (0 until numHashTables).map(i => new Hasher(hashFamily, numHashes))

    val radius = trainData.map({ sample =>
      data.map({ datum =>
        distance(sample.value, datum.value)
      }).max
    }).sum / trainData.length

    LSHIndexMetaData(hashTables, radius)
  }
}


object LSHIndexer {
  /**
   *
   * @param properties
   */
  def apply(properties : Map[String, String] = Map[String, String](), data: RDD[IndexerTuple[WorkingVector]]) : IndexGenerator = {
    val hashFamilyDescription = properties.getOrElse("hashFamily", "euclidean")
    val hashFamily = hashFamilyDescription.toLowerCase match {
      case "euclidean" => () => EuclideanHashFunction(2, 5, 3) //TODO: params
      case _ => null
    }

    val numHashTables = properties.getOrElse("numHashTables", "64").toInt
    val numHashes = properties.getOrElse("numHashes", "64").toInt

    val norm = properties.getOrElse("norm", "2").toInt

    val trainingSize = properties.getOrElse("trainingSize", "50000").toInt

    new LSHIndexer(hashFamily, numHashTables, numHashes, new NormBasedDistanceFunction(norm), trainingSize)
  }
}
