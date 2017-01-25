package org.vitrivr.adampro.query.distance

import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.query.distance.Distance._
import org.vitrivr.adampro.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  *
  * from Julia: 1 - sum(min(x, y)) / sum(max(x, y))
  */
object JaccardDistance extends DistanceFunction with Logging {
  val minDist = MinMaxDistFunc((a: VectorBase, b: VectorBase) => math.min(a, b))
  val maxDist = MinMaxDistFunc((a: VectorBase, b: VectorBase) => math.max(a, b))

  override def apply(v1: MathVector, v2: MathVector, weights: Option[MathVector]): Distance = {
    if (weights.isDefined) {
      log.warn("weights cannot be used with cosine distance and are ignored")
    }

    (1.0 - (minDist(v1, v2) / maxDist(v1, v2))).toFloat
  }

  case class MinMaxDistFunc(f: (VectorBase, VectorBase) => VectorBase) extends ElementwiseSummedDistanceFunction {
    override def element(v1: VectorBase, v2: VectorBase, w: VectorBase): Distance = f(v1, v2)
  }

}