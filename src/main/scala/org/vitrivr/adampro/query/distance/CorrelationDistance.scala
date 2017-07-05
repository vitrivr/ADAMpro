package org.vitrivr.adampro.query.distance

import breeze.stats.mean
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.query.distance.Distance._
import org.vitrivr.adampro.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  *
  * from Julia: cosine_dist(x - mean(x), y - mean(y))
  */
object CorrelationDistance extends DistanceFunction with Logging with Serializable {
  override def apply(v1: MathVector, v2: MathVector, weights: Option[MathVector]): Distance = {
    if (weights.isDefined) {
      log.warn("weights cannot be used with correlation distance and are ignored")
    }

    CosineDistance(v1 - mean(v1), v2 - mean(v2))
  }
}
