package ch.unibas.dmi.dbis.adam.cli.operations

import ch.unibas.dmi.dbis.adam.data.types.Feature.WorkingVector
import ch.unibas.dmi.dbis.adam.query.QueryHandler
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object SequentialQueryOp {
  /**
   *
   * @param tablename
   * @param query
   * @param k
   * @param distance
   */
  def apply(tablename: TableName, query : WorkingVector, k : Int, distance : DistanceFunction) : Unit = {
    val results = QueryHandler.sequentialQuery(query, distance, k, tablename).get()
    println(results.map(x => x.tid))
  }
}
