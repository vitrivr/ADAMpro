package org.vitrivr.adampro.query.distance

import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.query.distance.Distance._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  *
  * from Julia: sum(p .* log(p ./ q))
  */
object KullbackLeiblerDivergence extends ElementwiseSummedDistanceFunction with Serializable {
  override def element(v1: VectorBase, v2: VectorBase, w: VectorBase): Distance = {
    if (math.abs(v1) < 10E-6 || math.abs(v2) < 10E-6) {
      0.toFloat
    } else {
      (v1 * math.log(v1 / v2)).toFloat
    }
  }
}
