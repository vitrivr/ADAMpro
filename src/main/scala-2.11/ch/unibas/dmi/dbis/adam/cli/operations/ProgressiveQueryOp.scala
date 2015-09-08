package ch.unibas.dmi.dbis.adam.cli.operations

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
object ProgressiveQueryOp {
  /**
   *
   * @param tablename
   * @param query
   * @param k
   * @param distance
   */
  def apply(tablename: TableName, query : WorkingVector, k : Int, distance : NormBasedDistanceFunction) : Unit = {
    val results = QueryHandler.progressiveQuery(query, distance, k, tablename, processResults)
  }

  /**
   *
   * @param results
   */
  def processResults(results : Seq[Result]) : Unit = {
    println(results.mkString(", "))
  }
}
