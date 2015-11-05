package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
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
   * @param entityname
   * @param qv
   * @param k
   * @param distance
   */
  def apply(entityname: EntityName, qv : FeatureVector, k : Int, distance : DistanceFunction) : Seq[Result] = {
    val query = NearestNeighbourQuery(qv, distance, k)
    QueryHandler.sequentialQuery(entityname, query, None)
  }
}