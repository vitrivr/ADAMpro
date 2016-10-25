package ch.unibas.dmi.dbis.adam.query.distance

import breeze.linalg.max
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object NormBasedDistance {
  def apply(n: Double) = n match {
    case x if math.abs(n - 1) < 10E-6 => ManhattanDistance
    case x if math.abs(n - 2) < 10E-6 => EuclideanDistance
    case n => new MinkowskiDistance(n)
  }
}

/**
  * from Julia: sum(abs(x - y).^p .* w) ^ (1/p)
  *
  * @param n
  */
class MinkowskiDistance(val n: Double) extends ElementwiseSummedDistanceFunction with Serializable {
  @inline override def element(v1: VectorBase, v2: VectorBase, w: VectorBase): Distance = w * Math.pow(math.abs(v1 - v2), n).toFloat

  @inline override def normalize(cumSum: Distance): Distance = Math.pow(cumSum, 1 / n).toFloat
}

/**
  * from Julia: sum(abs(x - y) .* w)
  */
object ManhattanDistance extends MinkowskiDistance(1) with Serializable {
  @inline override def element(v1: VectorBase, v2: VectorBase, w: VectorBase): Distance = w * math.abs(v1 - v2)
}

/**
  * from Julia: sqrt(sum((x - y).^2 .* w))
  */
object EuclideanDistance extends MinkowskiDistance(2) with Serializable {
  @inline override def element(v1: VectorBase, v2: VectorBase, w: VectorBase): Distance = w * (v1 - v2) * (v1 - v2)

  override def equals(that: Any): Boolean = (this.getClass == that.getClass || that.getClass == SquaredEuclideanDistance.getClass)
  override def hashCode(): Int = 0
}

/**
  * from Julia: sum((x - y).^2)
  */
object SquaredEuclideanDistance extends MinkowskiDistance(2) with Serializable {
  @inline override def element(v1: VectorBase, v2: VectorBase, w: VectorBase): Distance = w * (v1 - v2) * (v1 - v2)
  @inline override def normalize(cumSum: Distance): Distance = cumSum

  override def equals(that: Any): Boolean = (this.getClass == that.getClass || that.getClass == EuclideanDistance.getClass)
  override def hashCode(): Int = 0
}

/**
  * from Julia: max(abs(x - y))
  */
object ChebyshevDistance extends MinkowskiDistance(Double.PositiveInfinity) with Serializable {
  override def apply(v1: FeatureVector, v2: FeatureVector, weights: Option[FeatureVector] = None): Distance = {
    if (weights.isDefined) {
      max((weights.get :* (v1 - v2)).map(_.abs))
    } else {
      max(((v1 - v2)).map(_.abs))
    }
  }
}