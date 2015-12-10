package ch.unibas.dmi.dbis.adam.index.structures.sh

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.MovableFeature
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.lsh.results.LSHResultHandler
import ch.unibas.dmi.dbis.adam.index.{BitStringIndexTuple, Index}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
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
class SHIndex(val indexname: IndexName, val entityname: EntityName, protected val df : DataFrame, private[index] val metadata: SHIndexMetaData)
  extends Index[BitStringIndexTuple] {

  override val indextypename: IndexTypeName = IndexStructures.SH
  override val confidence = 0.toFloat

  override protected def rdd : RDD[BitStringIndexTuple] = df.map(r => r : BitStringIndexTuple)

  override def scan(data : RDD[BitStringIndexTuple], q : FeatureVector, options : Map[String, Any], k : Int): HashSet[TupleID] = {
    val numOfQueries = options.getOrElse("numOfQ", "3").asInstanceOf[String].toInt

    import MovableFeature.conv_feature2MovableFeature
    val originalQuery = SHUtils.hashFeature(q, metadata)
    val queries = (List.fill(numOfQueries)(SHUtils.hashFeature(q.move(metadata.radius), metadata)) ::: List(originalQuery)).par

    val results = SparkStartup.sc.runJob(data, (context : TaskContext, tuplesIt : Iterator[BitStringIndexTuple]) => {
      val localRh = new LSHResultHandler(k)
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

    val globalResultHandler = new LSHResultHandler(k)
    globalResultHandler.offerResultElement(results.iterator)
    val ids = globalResultHandler.results.map(x => x.tid).toList
    HashSet(ids : _*)
  }
}


object SHIndex {
  def apply(indexname: IndexName, tablename: EntityName, data: DataFrame, meta: Any): SHIndex = {
    val indexMetaData = meta.asInstanceOf[SHIndexMetaData]
    new SHIndex(indexname, tablename, data, indexMetaData)
  }
}