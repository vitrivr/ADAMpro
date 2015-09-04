package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.data.IndexTuple
import ch.unibas.dmi.dbis.adam.data.types.Feature.WorkingVector
import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString
import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.{EuclideanHashFunction, Hasher, LSHashFunction}
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.rdd.RDD


class LSHIndexer(hashFamily : () => LSHashFunction, numHashTables : Int, numHashes : Int, distance : NormBasedDistanceFunction) extends IndexGenerator with Serializable {
  override val indextypename : String = "lsh"

  /**
   *
   */
  val hashTables : Seq[Hasher] = (0 until numHashTables).map(i => new Hasher(hashFamily, numHashes))

  /**
   *
   * @param indexname
   * @param tablename
   * @param data
   * @return
   */
  override def index(indexname : IndexName, tablename : TableName, data: RDD[IndexTuple[WorkingVector]]): Index = {
    val samples = data.takeSample(false, 200).toList

    val radius = samples.map({ sample =>
      data.map({ datum =>
        distance(sample.value, datum.value)
      }).max
    }).sum / samples.length

    val indexdata = data.map(
      datum => {
        val hash = hashFeature(datum.value)
        IndexTuple(datum.tid, hash)
      }
    )

    import SparkStartup.sqlContext.implicits._
    new LSHIndex(indexname, tablename, indexdata.toDF, hashTables, radius)
  }



  /**
   *
   * @param f
   * @return
   */
  @inline private def hashFeature(f : WorkingVector) : BitStringType = {
    val indices = hashTables.map(ht => ht(f))
    BitString.fromBitIndicesToSet(indices)
  }

}


object LSHIndexer {
  /**
   *
   * @param properties
   */
  def apply(properties : Map[String, String] = Map[String, String](), data: RDD[IndexTuple[WorkingVector]]) : IndexGenerator = {
    val hashFamilyDescription = properties.getOrElse("hashFamily", "euclidean")
    val hashFamily = hashFamilyDescription.toLowerCase match {
      case "euclidean" => () => EuclideanHashFunction(2, 5, 3) //TODO: params
      case _ => null
    }

    val numHashTables = properties.getOrElse("numHashTables", "64").toInt
    val numHashes = properties.getOrElse("numHashes", "64").toInt

    val norm = properties.getOrElse("norm", "2").toInt

    new LSHIndexer(hashFamily, numHashTables, numHashes, new NormBasedDistanceFunction(norm))
  }
}
