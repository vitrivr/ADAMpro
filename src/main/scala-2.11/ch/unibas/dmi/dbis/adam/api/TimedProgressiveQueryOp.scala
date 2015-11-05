package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import Feature._
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.{QueryHandler, Result}
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
   * @param tablename
   * @param query
   * @param k
   * @param distance
   */
  def apply(tablename: EntityName, query : FeatureVector, k : Int, distance : DistanceFunction, timelimit : Duration) : (Seq[Result], Float) = {
    QueryHandler.timedProgressiveQuery(query, distance, k, tablename, None, timelimit)
  }
}