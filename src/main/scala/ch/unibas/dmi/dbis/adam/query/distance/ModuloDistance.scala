package ch.unibas.dmi.dbis.adam.query.distance

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.Distance._

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