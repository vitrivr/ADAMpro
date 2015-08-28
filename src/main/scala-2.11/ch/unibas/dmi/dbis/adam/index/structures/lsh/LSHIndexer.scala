package ch.unibas.dmi.dbis.adam.index.structures.lsh

import breeze.linalg.DenseVector
import ch.unibas.dmi.dbis.adam.data.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.data.types.Feature.WorkingVector
import ch.unibas.dmi.dbis.adam.data.{IndexMetaBuilder, IndexTuple}
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.{EuclideanHashFunction, Hasher, LSHashFunction}
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator, IndexScanner}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.rdd.RDD

import scala.util.Random


class LSHIndexer(hashFamily : () => LSHashFunction, numHashTables : Int, numHashes : Int, distance : NormBasedDistanceFunction) extends IndexGenerator with IndexScanner with Serializable {
  override val indexname : String = "lsh"

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

    val indexMetaBuilder = new IndexMetaBuilder()
    indexMetaBuilder.put("radius", radius)
    indexMetaBuilder.put("hashtables", hashTables)

    import SparkStartup.sqlContext.implicits._
    Index(indexname, tablename, indexdata.toDF, indexMetaBuilder.build())
  }

  /**
   *
   * @param q
   * @param index
   * @param options
   * @return
   */
  override def query(q : WorkingVector, index : Index, options : Map[String, Any]) : Seq[TupleID] = {
    val data = index.index
    val metadata = index.indexMeta

    val hashTables : Seq[Hasher] = metadata.get("hashtables")
    val radius : Float = metadata.get("radius")

    val k = options("k").asInstanceOf[Integer]

    val extendedQuery = getExtendedQuery(q, radius)


    val results = data
      .map{ tuple => IndexTuple(tuple.getLong(0), tuple.getSeq[Int](1)) }
      .filter { indexTuple =>
      indexTuple.value.zip(extendedQuery).exists({
        case (indexHash, acceptedValues) => acceptedValues.contains(indexHash)
      })
    }.map { indexTuple => indexTuple.tid }

    results.take(k * 2)
  }

  /**
   *
   * @param q
   * @param radius
   * @return
   */
  private def getExtendedQuery(q : WorkingVector, radius : Float) = {
    val queries = (1 to 10).map(x => q.move(radius)).:+(q)
    val hashes = queries.map(qVector => hashTables.map(ht => ht(qVector)))

    val hashesPerDim = (0 until q.length).map { i =>
      hashes.map { hash => hash(i) }.distinct
    }

    hashesPerDim
  }

  /**
   *
   * @param d
   */
  //TODO use moveable vector
  implicit class MovableDenseVector(d : WorkingVector) {
    def move(radius : Float) : WorkingVector = {
      val diff = DenseVector.fill(d.length)(radius - 2 * radius * Random.nextFloat)
      d + diff
    }
  }

  /**
   *
   * @param f
   * @return
   */
  @inline private def hashFeature(f : WorkingVector) : Seq[Int] = {
    hashTables.map(ht => ht(f))
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
