package org.vitrivr.adampro.query.distance

import org.vitrivr.adampro.datatypes.vector.Vector.VectorBase
import org.vitrivr.adampro.query.distance.Distance.Distance

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  *
  * from Julia: sum((x .!= y) .* w)
  */
object HammingDistance extends ElementwiseSummedDistanceFunction with Serializable {
  override def element(v1: VectorBase, v2: VectorBase, w: VectorBase): Distance = {
    (w * (if (math.abs(v1 - v2) > 10E-6) {
      1.0
    } else {
      0.0
    })).toFloat
  }
}
