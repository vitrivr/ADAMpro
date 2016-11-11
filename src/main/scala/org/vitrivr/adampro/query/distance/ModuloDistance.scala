package org.vitrivr.adampro.query.distance

import org.vitrivr.adampro.datatypes.feature.Feature._
import org.vitrivr.adampro.query.distance.Distance._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
object ModuloDistance  extends ElementwiseSummedDistanceFunction with Serializable {
  override def element(v1_q: VectorBase, v2: VectorBase, w: VectorBase): Distance = {
    w * (v2 % v1_q)
  }
}