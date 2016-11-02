package ch.unibas.dmi.dbis.adam.query.distance

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.Distance._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2015
  */
@SerialVersionUID(100L)
trait DistanceFunction extends Serializable {
  def apply(v1_q: FeatureVector, v2: FeatureVector, weights: Option[FeatureVector] = None): Distance
}
