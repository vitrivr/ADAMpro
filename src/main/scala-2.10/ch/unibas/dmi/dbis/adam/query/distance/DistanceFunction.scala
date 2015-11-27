package ch.unibas.dmi.dbis.adam.query.distance

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.Distance._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait DistanceFunction {
  /**
   *
   * @param v1
   * @param v2
   * @return
   */
  def apply(v1: FeatureVector, v2: FeatureVector): Distance
}
