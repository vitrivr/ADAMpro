package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.{QueryHandler, Result}
import ch.unibas.dmi.dbis.adam.entity.Entity._

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
  def apply(tablename: EntityName, query : FeatureVector, k : Int, distance : DistanceFunction) : Seq[Result] = {
    QueryHandler.sequentialQuery(query, distance, k, tablename, None)
  }
}
