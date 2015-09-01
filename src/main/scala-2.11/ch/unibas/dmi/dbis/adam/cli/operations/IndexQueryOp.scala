package ch.unibas.dmi.dbis.adam.cli.operations

import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.query.QueryHandler
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
//TODO: replace this by query
object IndexQueryOp {
  def apply(tablename: TableName, query : WorkingVector, k : Int, distance : DistanceFunction) : Unit = {
    val results = QueryHandler.query(query, distance, k, tablename)
    println(results.map(x => x.tid))
  }
}
