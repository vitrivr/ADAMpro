package ch.unibas.dmi.dbis.adam.index.structures.sh

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.MovableFeature
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.utils.ResultHandler
import ch.unibas.dmi.dbis.adam.index.{BitStringIndexTuple, Index}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, MinkowskiDistance}
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class SHIndex(val indexname: IndexName, val entityname: EntityName, private[index]  val df : DataFrame, private[index] val metadata: SHIndexMetaData)(@transient implicit val ac : AdamContext)
  extends Index {

  override val indextype: IndexTypeName = IndexTypes.SHINDEX

  override val lossy: Boolean = true
  override val confidence = 0.toFloat

  override def scan(data : DataFrame, q : FeatureVector, distance : DistanceFunction, options : Map[String, Any], k : Int): Set[Result] = {
    log.debug("scanning SH index " + indexname)

    val numOfQueries = options.getOrElse("numOfQ", "3").asInstanceOf[String].toInt

    import MovableFeature.conv_feature2MovableFeature
    val originalQuery = SHUtils.hashFeature(q, metadata)
    val queries = ac.sc.broadcast(List.fill(numOfQueries)(SHUtils.hashFeature(q.move(metadata.radius), metadata)) ::: List(originalQuery))

    val rdd = data.map(r => r : BitStringIndexTuple)

    val maxScore : Float = originalQuery.intersectionCount(originalQuery) * numOfQueries
    
    val results = ac.sc.runJob(rdd, (context : TaskContext, tuplesIt : Iterator[BitStringIndexTuple]) => {
      val localRh = new ResultHandler[Int](k)

      while (tuplesIt.hasNext) {
        val tuple = tuplesIt.next()

        var i = 0
        var score = 0
        while (i < queries.value.length) {
          val query = queries.value(i)
          score += tuple.value.intersectionCount(query)
          i += 1
        }

        localRh.offer(tuple, score)
      }

      localRh.results.toSeq
    }).flatten.sortBy(- _.score)

    log.debug("SH index sub-results sent to global result handler")

    val globalResultHandler = new ResultHandler[Int](k)
    results.foreach(result => globalResultHandler.offer(result))
    val ids = globalResultHandler.results

    log.debug("SH index returning " + ids.length + " tuples")
    ids.map(result => Result(result.score.toFloat / maxScore, result.tid)).toSet
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
  def apply(indexname: IndexName, tablename: EntityName, data: DataFrame, meta: Any)(implicit ac : AdamContext): SHIndex = {
    val indexMetaData = meta.asInstanceOf[SHIndexMetaData]
    new SHIndex(indexname, tablename, data, indexMetaData)
  }
}