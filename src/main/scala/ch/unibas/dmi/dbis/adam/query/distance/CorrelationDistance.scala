package ch.unibas.dmi.dbis.adam.query.distance

import breeze.linalg.norm
import breeze.stats.mean
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.Distance._
import ch.unibas.dmi.dbis.adam.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  *
  * from Julia: cosine_dist(x - mean(x), y - mean(y))
  */
object CorrelationDistance extends DistanceFunction with Logging with Serializable {
  override def apply(v1: FeatureVector, v2: FeatureVector, weights: Option[FeatureVector]): Distance = {
    if (weights.isDefined) {
      log.warn("weights cannot be used with correlation distance and are ignored")
    }

    val meanv1 = mean(v1)
    val meanv2 = mean(v2)

    ((1.0 - ((v1 - meanv1) dot (v2 - meanv2))) / (norm(v1 - meanv1, 2.0) * norm(v2 - meanv2, 2.0))).toFloat
  }
}
