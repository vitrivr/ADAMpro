package ch.unibas.dmi.dbis.adam.query.distance

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.Distance._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  *
  * from Julia: sum((x - y).^2 / (x + y))
  */
object ChiSquaredDistance extends ElementwiseSummedDistanceFunction with Serializable {
  override def element(v1: VectorBase, v2: VectorBase, w: VectorBase): Distance = {
    (w * math.pow(v1 - v2, 2.0) / (v1 + v2)).toFloat
  }
}
