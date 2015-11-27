package ch.unibas.dmi.dbis.adam.query.distance

import breeze.linalg.DenseVector
import breeze.linalg.functions._
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import Feature.{VectorBase, FeatureVector}
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class MinkowskiDistance(val n : Double) extends DistanceFunction with Serializable {
  override def apply(v1: FeatureVector, v2: FeatureVector): Distance =  minkowskiDistance(v1, v2, n).toFloat
  def apply(v1: VectorBase, v2: VectorBase): Distance = minkowskiDistance(DenseVector(v1), DenseVector(v2), n).toFloat
}

object ManhattanDistance extends DistanceFunction with Serializable {
  override def apply(v1: FeatureVector, v2: FeatureVector): Distance =  manhattanDistance(v1, v2).toFloat
}

object EuclideanDistance extends DistanceFunction with Serializable {
  override def apply(v1: FeatureVector, v2: FeatureVector): Distance =  euclideanDistance(v1, v2).toFloat
}

object ChebyshevDistance extends DistanceFunction with Serializable {
  override def apply(v1: FeatureVector, v2: FeatureVector): Distance =  chebyshevDistance(v1, v2).toFloat
}