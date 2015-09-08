package ch.unibas.dmi.dbis.adam.cli.operations

import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.query.QueryHandler
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction

import scala.collection.mutable.{Map => mMap}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object IndexQueryOp {
  def apply(indexname : IndexName, query : WorkingVector, k : Int, distance : NormBasedDistanceFunction) : Unit = {
    val options = mMap[String, String]()
    options += "k" -> k.toString
    options += "norm" -> distance.n.toString

    val results = QueryHandler.indexQuery(query, distance, k, indexname, options.toMap).get()
    println(results.map(x => x.tid))
  }
}
