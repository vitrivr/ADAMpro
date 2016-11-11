package org.vitrivr.adampro.query.distance

import org.vitrivr.adampro.datatypes.feature.Feature._
import org.vitrivr.adampro.query.distance.Distance._

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
