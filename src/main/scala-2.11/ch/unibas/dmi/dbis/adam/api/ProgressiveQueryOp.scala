package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.query.handler.NearestNeighbourQueryHandler
import ch.unibas.dmi.dbis.adam.query.progressive.ProgressiveQueryStatus
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.entity.Entity._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object ProgressiveQueryOp {
  /**
   *
   * @param entityname
   * @param qv
   * @param k
   * @param distance
   */
  def apply(entityname: EntityName, qv : FeatureVector, k : Int, distance : DistanceFunction, onComplete : (ProgressiveQueryStatus.Value, Seq[Result], Float, Map[String, String]) => Unit) : Int = {
    val query = NearestNeighbourQuery(qv, distance, k)
    NearestNeighbourQueryHandler.progressiveQuery(entityname, query, None, onComplete)
  }
}
