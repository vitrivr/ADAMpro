package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.query.{QueryHandler, Result}
import ch.unibas.dmi.dbis.adam.table.Table._

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
  def apply(tablename: TableName, query : WorkingVector, k : Int, distance : NormBasedDistanceFunction, timelimit : Duration) : (Seq[Result], Float) = {
    QueryHandler.timedProgressiveQuery(query, distance, k, tablename, None, timelimit)
  }
}