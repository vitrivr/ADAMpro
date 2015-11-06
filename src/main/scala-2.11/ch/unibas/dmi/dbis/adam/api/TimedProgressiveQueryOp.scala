package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import Feature._
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.handler.NearestNeighbourQueryHandler
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.entity.Entity._

import scala.concurrent.duration.Duration

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
object TimedProgressiveQueryOp {
  /**
   *
   * @param entityname
   * @param qv
   * @param k
   * @param distance
   */
  def apply(entityname: EntityName, qv : FeatureVector, k : Int, distance : DistanceFunction, timelimit : Duration) : (Seq[Result], Float) = {
    val query = NearestNeighbourQuery(qv, distance, k)
    NearestNeighbourQueryHandler.timedProgressiveQuery(entityname, query, None, timelimit)
  }
}