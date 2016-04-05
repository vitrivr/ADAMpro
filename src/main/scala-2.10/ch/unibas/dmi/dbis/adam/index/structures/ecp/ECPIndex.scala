package ch.unibas.dmi.dbis.adam.index.structures.ecp

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
class ECPIndex(val indexname: IndexName, val entityname: EntityName, protected val df: DataFrame, private[index] val metadata : ECPIndexMetaData)
  extends Index {

  override val indextype: IndexTypeName = IndexTypes.ECPINDEX

  override val lossy: Boolean = true
  override val confidence = 0.toFloat

  override def scan(data : DataFrame, q : FeatureVector, distance : DistanceFunction, options : Map[String, Any], k : Int): Set[Result] = {
    log.debug("scanning eCP index " + indexname)

    val centroids = metadata.leaders.map(l => {
      (l.id, metadata.distance(q, l.feature))
    }).sortBy(_._2)

    val rdd = data.map(r => r : LongIndexTuple)

    val ids = SparkStartup.sc.runJob(rdd, (context : TaskContext, tuplesIt : Iterator[LongIndexTuple]) => {
      var results = ListBuffer[Result]()
      var i = 0
      while(i < centroids.length && results.length < k){
        results ++= tuplesIt.filter(_.value == centroids(i)._1).map(tuple => Result(centroids(i)._2,tuple.id)).toSeq
        i += 1
      }
      results.toSeq
    }).flatten

    log.debug("eCP index returning " + ids.length + " tuples")

    ids.toSet
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = true
}

object ECPIndex {
  def apply(indexname: IndexName, tablename: EntityName, data: DataFrame, meta: Any): ECPIndex = {
    val indexMetaData = meta.asInstanceOf[ECPIndexMetaData]
    new ECPIndex(indexname, tablename, data, indexMetaData)
  }
}