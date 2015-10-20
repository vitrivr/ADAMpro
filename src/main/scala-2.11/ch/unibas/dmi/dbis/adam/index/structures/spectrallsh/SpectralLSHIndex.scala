package ch.unibas.dmi.dbis.adam.index.structures.spectrallsh

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.MovableFeature
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.spectrallsh.results.SpectralLSHResultHandler
import ch.unibas.dmi.dbis.adam.index.{BitStringIndexTuple, Index}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.table.Table._
import ch.unibas.dmi.dbis.adam.table.Tuple.TupleID
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.HashSet


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class SpectralLSHIndex(val indexname: IndexName, val tablename: TableName, protected val indexdata: DataFrame, private val indexMetaData: SpectralLSHIndexMetaData)
  extends Index[BitStringIndexTuple] {
  override val indextypename: IndexTypeName = "slsh"
  override val precise = false

  /**
   *
   * @return
   */
  override protected def indexToTuple : RDD[BitStringIndexTuple] = {
    indexdata
      .map { tuple =>
      BitStringIndexTuple(tuple.getLong(0), tuple.getAs[BitString[_]](1))
    }
  }

  /**
   *
   * @param q
   * @param options
   * @return
   */
  override def scan(q: WorkingVector, options: Map[String, String], filter : Option[HashSet[TupleID]], queryID : Option[String]): HashSet[TupleID] = {
    val k = options("k").toInt
    val numOfQueries = options.getOrElse("numOfQ", "3").toInt

    import MovableFeature.conv_feature2MovableFeature
    val originalQuery = SpectralLSHUtils.hashFeature(q, indexMetaData)
    val queries = (List.fill(numOfQueries)(SpectralLSHUtils.hashFeature(q.move(indexMetaData.radius), indexMetaData)) ::: List(originalQuery)).par

    SparkStartup.sc.setLocalProperty("spark.scheduler.pool", "index")
    SparkStartup.sc.setJobGroup(queryID.getOrElse(""), indextypename, true)

    val results = SparkStartup.sc.runJob(getIndexTuples(filter), (context : TaskContext, tuplesIt : Iterator[BitStringIndexTuple]) => {
      val localRh = new SpectralLSHResultHandler(k)
      while (tuplesIt.hasNext) {
        val tuple = tuplesIt.next()

        var i = 0
        var score = 0
        while (i < queries.length) {
          val query = queries(i)
          score += tuple.value.intersectionCount(query)
          i += 1
        }

        localRh.offerIndexTuple(tuple, score)
      }

      localRh.results.toSeq
    }).flatten


    val globalResultHandler = new SpectralLSHResultHandler(k)
    globalResultHandler.offerResultElement(results.iterator)
    val ids = globalResultHandler.results.map(x => x.tid).toList

    HashSet(ids : _*)
  }

  /**
   *
   * @return
   */
  override private[index] def getMetadata(): Serializable = {
    indexMetaData
  }
}


object SpectralLSHIndex {
  /**
   *
   * @param indexname
   * @param tablename
   * @param data
   * @param meta
   * @return
   */
  def apply(indexname: IndexName, tablename: TableName, data: DataFrame, meta: Any): SpectralLSHIndex = {
    val indexMetaData = meta.asInstanceOf[SpectralLSHIndexMetaData]
    new SpectralLSHIndex(indexname, tablename, data, indexMetaData)
  }
}