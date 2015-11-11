package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.handler.QueryHandler
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
object QueryOp {
  /**
   *
   * @param entityname
   * @param qv
   * @param k
   * @param distance
   */
  def apply(entityname: EntityName, qv : FeatureVector, k : Int, distance : DistanceFunction) : Seq[Result] = {
    val nnq = NearestNeighbourQuery(qv, distance, k)
    QueryHandler.query(entityname, None, nnq, None, false)
  }
}
