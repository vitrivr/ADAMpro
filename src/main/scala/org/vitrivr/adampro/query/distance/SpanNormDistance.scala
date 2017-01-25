package org.vitrivr.adampro.query.distance

import breeze.linalg.{max, min}
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.query.distance.Distance._
import org.vitrivr.adampro.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  *
  * from Julia: max(x - y) - min(x - y)
  */
object SpanNormDistance extends DistanceFunction with Logging with Serializable {
  override def apply(v1: MathVector, v2: MathVector, weights: Option[MathVector]): Distance = {
    if (weights.isDefined) {
      log.error("weights cannot be used with span norm distance and are ignored")
    }

    max(v1 - v2) - min(v1 - v2)
  }
}