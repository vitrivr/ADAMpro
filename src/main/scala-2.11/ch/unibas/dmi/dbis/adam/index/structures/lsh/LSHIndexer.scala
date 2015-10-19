package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.datatypes.Feature.WorkingVector
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.{EuclideanHashFunction, Hasher}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.rdd.RDD


class LSHIndexer(hashFamily : String, numHashTables : Int, numHashes : Int, distance : NormBasedDistanceFunction, trainingSize : Int) extends IndexGenerator with Serializable {
  override val indextypename : String = "lsh"

  /**
   *
   * @param indexname
   * @param tablename
   * @param data
   * @return
   */
  override def index(indexname : IndexName, tablename : TableName, data: RDD[IndexerTuple[WorkingVector]]): Index[_ <: IndexTuple] = {
    val indexMetaData = train(data)

    val indexdata = data.map(
      datum => {
        val hash = LSHUtils.hashFeature(datum.value, indexMetaData)
        BitStringIndexTuple(datum.tid, hash)
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
    val trainData2 = data.takeSample(true, trainingSize).par

    val radius = trainData.map({ sample =>
      var max = 0.toFloat
      val it = trainData2.iterator
      while(it.hasNext){
        val datum = it.next()
        val d = distance(sample.value, datum.value)

        if(d > max){
          max = d
        }
      }
      max
    }).sum / trainData.length

    //TODO: move to apply
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
  def apply(properties : Map[String, String] = Map[String, String](), data: RDD[IndexerTuple[WorkingVector]]) : IndexGenerator = {
    //val totalNumBits = properties.getOrElse("totalNumBits", (data.first.value.length * 8).toString).toInt

    val hashFamilyDescription = properties.getOrElse("hashFamily", "euclidean")
    val hashFamily = hashFamilyDescription.toLowerCase match {
      case "euclidean" => "euclidean" //TODO: params
      case _ => null
    }

    val numHashTables = properties.getOrElse("numHashTables", "64").toInt
    val numHashes = properties.getOrElse("numHashes", "64").toInt

    val norm = properties.getOrElse("norm", "2").toInt

    val trainingSize = properties.getOrElse("trainingSize", "5000").toInt

    new LSHIndexer(hashFamily, numHashTables, numHashes, new NormBasedDistanceFunction(norm), trainingSize)
  }
}
