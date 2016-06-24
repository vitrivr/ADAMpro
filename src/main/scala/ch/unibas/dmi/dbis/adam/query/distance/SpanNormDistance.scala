package ch.unibas.dmi.dbis.adam.query.distance

import breeze.linalg.{max, min}
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.Distance._
import ch.unibas.dmi.dbis.adam.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  *
  * from Julia: max(x - y) - min(x - y)
  */
object SpanNormDistance extends DistanceFunction with Logging with Serializable {
  override def apply(v1: FeatureVector, v2: FeatureVector, weights: Option[FeatureVector]): Distance = {
    if (weights.isDefined) {
      log.error("weights cannot be used with span norm distance and are ignored")
    }

    max(v1 - v2) - min(v1 - v2)
  }
}