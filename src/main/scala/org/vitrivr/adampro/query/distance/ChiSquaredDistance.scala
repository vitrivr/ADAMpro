package org.vitrivr.adampro.query.distance

import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.query.distance.Distance._

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
    if(v1 + v2 > 0) { w * math.pow(v1 - v2, 2.0) / (v1 + v2) }.toFloat else { 0 }
  }
}
