package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.Feature.WorkingVector
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.query.{ProgressiveQueryStatus, QueryHandler, Result}
import ch.unibas.dmi.dbis.adam.table.Table._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object ProgQueryOp {
  /**
   *
   * @param tablename
   * @param query
   * @param k
   * @param distance
   */
  def apply(tablename: TableName, query : WorkingVector, k : Int, distance : NormBasedDistanceFunction, onComplete : (ProgressiveQueryStatus.Value, Seq[Result], Map[String, String]) => Unit) : Int = {
    QueryHandler.progressiveQuery(query, distance, k, tablename, None, onComplete)
  }
}
