package org.vitrivr.adampro.query.distance

import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.query.distance.Distance._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2015
  */
@SerialVersionUID(100L)
trait DistanceFunction extends Serializable {
  def apply(v1_q: MathVector, v2: MathVector, weights: Option[MathVector] = None): Distance
}
