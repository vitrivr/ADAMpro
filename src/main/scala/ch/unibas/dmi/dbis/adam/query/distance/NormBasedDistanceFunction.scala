package ch.unibas.dmi.dbis.adam.query.distance

import breeze.linalg.{zipValues, DenseVector}
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.{FeatureVector, VectorBase}
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance
import org.apache.spark.Logging

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object NormBasedDistanceFunction extends Serializable {
  def apply(n: Double) = n match {
    case x if math.abs(n - 1) < 0.00001 => ManhattanDistance
    case x if math.abs(n - 2) < 0.00001 => EuclideanDistance
    case n => new MinkowskiDistance(n)
  }
}

@SerialVersionUID(100L)
class MinkowskiDistance(val n: Double) extends DistanceFunction with Logging with Serializable {
  override def apply(v1: FeatureVector, v2: FeatureVector, weights: Option[FeatureVector] = None): Distance = {
    var cum = 0.0
    if (weights.isEmpty) {
      log.trace("compute distance")
      zipValues(v1, v2).foreach { (a, b) =>
        cum += Math.pow(math.abs(a - b), n)
      }
    } else {
      log.trace("compute distance with weights")
      val itv1 = v1.valuesIterator
      val itv2 = v2.valuesIterator
      val itw = weights.get.valuesIterator

      while (itv1.hasNext && itv2.hasNext && itw.hasNext) {
        cum += Math.pow(itw.next() * math.abs(itv1.next() - itv2.next()), n)
      }
    }

    Math.pow(cum, 1 / n).toFloat
  }

  def apply(v1: VectorBase, v2: VectorBase): Distance = apply(DenseVector(v1), DenseVector(v2))
}

@SerialVersionUID(100L)
object ManhattanDistance extends MinkowskiDistance(1) with Logging with Serializable {}

@SerialVersionUID(100L)
object EuclideanDistance extends MinkowskiDistance(2) with Logging with Serializable {}