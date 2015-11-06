package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.handler.NearestNeighbourQueryHandler
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.query.Result

import scala.collection.mutable.{Map => mMap}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object IndexQueryOp {
  def apply(indexname : IndexName, qv : FeatureVector, k : Int, distance : DistanceFunction) : Seq[Result] = {
    val query = NearestNeighbourQuery(qv, distance, k)
    NearestNeighbourQueryHandler.indexQuery(indexname, query, None)
  }
}
