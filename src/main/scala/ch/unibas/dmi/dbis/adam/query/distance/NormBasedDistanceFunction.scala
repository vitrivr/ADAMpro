package ch.unibas.dmi.dbis.adam.query.distance

import breeze.linalg.DenseVector
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.{FeatureVector, SparseFeatureVector, VectorBase}
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

    //computing sum
    if (weights.isEmpty) {
      //un-weighted
      if (v1.isInstanceOf[SparseFeatureVector] && v2.isInstanceOf[SparseFeatureVector]) {
        //sparse vectors
        log.trace("compute distance without weights, sparse vectors")

        val sv1 = v1.asInstanceOf[SparseFeatureVector]
        val sv2 = v2.asInstanceOf[SparseFeatureVector]

        var offset = 0
        while (offset < sv1.activeSize) {
          if (sv1.isActive(offset) && sv2.isActive(offset)) {
            cum += Math.pow(math.abs(sv1.valueAt(offset) - sv2.valueAt(offset)), n)
          } else if (sv1.isActive(offset)) {
            cum += Math.pow(sv1.valueAt(offset), n)
          } else if (sv2.isActive(offset)) {
            cum += Math.pow(sv2.valueAt(offset), n)
          }
          offset += 1
        }
      } else {
        //dense vectors
        log.trace("compute distance without weights, dense vectors")

        var offset = 0
        while (offset < v1.length) {
          cum += Math.pow(math.abs(v1(offset) - v2(offset)), n)
          offset += 1
        }
      }
    } else {
      //weighted
      log.trace("compute distance with weights")

      if (weights.get.isInstanceOf[SparseFeatureVector]) {
        //sparse weights
        log.trace("compute distance with sparse weights")

        val sweights = weights.get.asInstanceOf[SparseFeatureVector]

        var offset = 0
        while (offset < sweights.activeSize) {
          if (sweights.isActive(offset)) {
            cum += Math.pow(sweights.valueAt(offset) * math.abs(v1(offset) - v2(offset)), n)
          }
          offset += 1
        }
      } else if(v1.isInstanceOf[SparseFeatureVector] && v2.isInstanceOf[SparseFeatureVector]){
        //dense weights, sparse vectors
        log.trace("compute distance with dense weights and sparse vectors")

        val sv1 = v1.asInstanceOf[SparseFeatureVector]
        val sv2 = v2.asInstanceOf[SparseFeatureVector]

        var offset = 0
        while (offset < sv1.activeSize) {
          if (sv1.isActive(offset) && sv2.isActive(offset)) {
            cum += Math.pow(weights.get(offset) * math.abs(sv1.valueAt(offset) - sv2.valueAt(offset)), n)
          } else if (sv1.isActive(offset)) {
            cum += Math.pow(weights.get(offset) * sv1.valueAt(offset), n)
          } else if (sv2.isActive(offset)) {
            cum += Math.pow(weights.get(offset) * sv2.valueAt(offset), n)
          }
          offset += 1
        }
      } else {
        //dense weights, dense vectors
        log.trace("compute distance with dense weights and dense vectors")

        var offset = 0
        while (offset < v1.length) {
          cum += Math.pow(weights.get(offset) * math.abs(v1(offset) - v2(offset)), n)
          offset += 1
        }
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