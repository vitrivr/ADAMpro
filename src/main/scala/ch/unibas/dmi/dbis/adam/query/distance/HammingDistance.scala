package ch.unibas.dmi.dbis.adam.query.distance

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.VectorBase
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance

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
