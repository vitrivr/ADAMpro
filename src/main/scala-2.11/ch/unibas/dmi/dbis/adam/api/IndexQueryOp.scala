package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.{QueryHandler, Result}

import scala.collection.mutable.{Map => mMap}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object IndexQueryOp {
  def apply(indexname : IndexName, query : FeatureVector, k : Int, distance : DistanceFunction) : Seq[Result] = {
    val options = mMap[String, String]()

    QueryHandler.indexQuery(query, distance, k, indexname, options.toMap, None)
  }
}
