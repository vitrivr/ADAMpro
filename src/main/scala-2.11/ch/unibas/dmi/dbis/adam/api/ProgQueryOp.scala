package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.query.{QueryHandler, Result}
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
  def apply(tablename: TableName, query : WorkingVector, k : Int, distance : NormBasedDistanceFunction, onComplete : (Seq[Result]) => Unit) : Int = {
    QueryHandler.progressiveQuery(query, distance, k, tablename, onComplete)
  }
}
