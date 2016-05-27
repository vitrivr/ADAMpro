package ch.unibas.dmi.dbis.adam.query.distance

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.Distance._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
@SerialVersionUID(100L)
trait DistanceFunction {
  def apply(v1: FeatureVector, v2: FeatureVector, weights : Option[FeatureVector] = None): Distance
}
