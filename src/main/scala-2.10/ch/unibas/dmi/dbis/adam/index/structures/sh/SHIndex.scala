package ch.unibas.dmi.dbis.adam.index.structures.sh

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.MovableFeature
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.lsh.results.LSHResultHandler
import ch.unibas.dmi.dbis.adam.index.{BitStringIndexTuple, Index}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.MinkowskiDistance
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class SHIndex(val indexname: IndexName, val entityname: EntityName, protected val df : DataFrame, private[index] val metadata: SHIndexMetaData)
  extends Index {

  override val indextype: IndexTypeName = IndexTypes.SHINDEX

  override val lossy: Boolean = true
  override val confidence = 0.toFloat

  override def scan(data : DataFrame, q : FeatureVector, options : Map[String, Any], k : Int): Set[Result] = {
    log.debug("scanning SH index " + indexname)

    val numOfQueries = options.getOrElse("numOfQ", "3").asInstanceOf[String].toInt

    import MovableFeature.conv_feature2MovableFeature
    val originalQuery = SHUtils.hashFeature(q, metadata)
    val queries = (List.fill(numOfQueries)(SHUtils.hashFeature(q.move(metadata.radius), metadata)) ::: List(originalQuery)).par

    val rdd = data.map(r => r : BitStringIndexTuple)

    val maxScore = originalQuery.intersectionCount(originalQuery) * numOfQueries

    val results = SparkStartup.sc.runJob(rdd, (context : TaskContext, tuplesIt : Iterator[BitStringIndexTuple]) => {
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

    log.debug("SH index sub-results sent to global result handler")

    val globalResultHandler = new LSHResultHandler(k)
    globalResultHandler.offerResultElement(results.iterator)
    val ids = globalResultHandler.results

    log.debug("SH index returning " + ids.length + " tuples")
    ids.map(result => Result(result.score.toFloat / maxScore.toFloat, result.tid)).toSet
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = {
    //is this check correct?
    if(nnq.distance.isInstanceOf[MinkowskiDistance]){
      return true
    }

    false
  }
}


object SHIndex {
  def apply(indexname: IndexName, tablename: EntityName, data: DataFrame, meta: Any): SHIndex = {
    val indexMetaData = meta.asInstanceOf[SHIndexMetaData]
    new SHIndex(indexname, tablename, data, indexMetaData)
  }
}