package ch.unibas.dmi.dbis.adam.query.distance

import breeze.linalg.{Vector, max}
import breeze.numerics.abs
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.CorrelationDistance._
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object NormBasedDistance {
  def apply(n: Double) = n match {
    case x if math.abs(n - 1) < 0.00001 => ManhattanDistance
    case x if math.abs(n - 2) < 0.00001 => EuclideanDistance
    case n => new MinkowskiDistance(n)
  }
}

/**
  * from Julia: sum(abs(x - y).^p .* w) ^ (1/p)
  *
  * @param n
  */
class MinkowskiDistance(val n: Double) extends ElementwiseSummedDistanceFunction with Serializable {
  override def element(v1: VectorBase, v2: VectorBase, w: VectorBase): Distance = Math.pow(w * math.abs(v1 - v2), n).toFloat

  override def normalize(cumSum: Distance): Distance = Math.pow(cumSum, 1 / n).toFloat
}

/**
  * from Julia: sum(abs(x - y) .* w)
  */
object ManhattanDistance extends MinkowskiDistance(1) with Serializable {}

/**
  * from Julia: sqrt(sum((x - y).^2 .* w))
  */
object EuclideanDistance extends MinkowskiDistance(2) with Serializable {}

/**
  * from Julia: sum((x - y).^2)
  */
object SquaredEuclideanDistance extends MinkowskiDistance(2) with Serializable {
  override def normalize(cumSum: Distance): Distance = cumSum
}

/**
  * from Julia: max(abs(x - y))
  */
object ChebyshevDistance extends MinkowskiDistance(Double.PositiveInfinity) with Serializable {
  override def apply(v1: FeatureVector, v2: FeatureVector, weights: Option[FeatureVector]): Distance = {
    if (weights.isDefined) {
      max((weights.get :* (v1 - v2)).map(_.abs))
    } else {
      max(((v1 - v2)).map(_.abs))
    }
  }
}