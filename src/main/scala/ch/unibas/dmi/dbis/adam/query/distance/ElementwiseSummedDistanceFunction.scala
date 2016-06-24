package ch.unibas.dmi.dbis.adam.query.distance

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance
import ch.unibas.dmi.dbis.adam.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
trait ElementwiseSummedDistanceFunction extends DistanceFunction with Logging with Serializable {

  override def apply(v1: FeatureVector, v2: FeatureVector, weights: Option[FeatureVector]): Distance = {
    var cumSum = 0.0

    //computing sum
    if (weights.isEmpty) {
      //un-weighted
      if (v1.isInstanceOf[SparseFeatureVector] && v2.isInstanceOf[SparseFeatureVector]) {
        //sparse vectors
        log.trace("compute distance without weights, sparse vectors")

        val sv1 = v1.asInstanceOf[SparseFeatureVector]
        val sv2 = v2.asInstanceOf[SparseFeatureVector]
        var offset = 0

        while (offset < math.max(sv1.activeSize, sv2.activeSize)) {
          if (offset < sv1.activeSize && offset < sv2.activeSize) {
            cumSum += element(sv1.valueAt(offset), sv2.valueAt(offset))
          } else if (offset < sv1.activeSize) {
            cumSum += element(sv1.valueAt(offset), 0.0)
          } else if (offset < sv2.activeSize) {
            cumSum += element(0.0, sv2.valueAt(offset))
          }
          offset += 1
        }
      } else {
        //dense vectors
        log.trace("compute distance without weights, dense vectors")

        var offset = 0
        while (offset < math.min(v1.length, v2.length)) {
          cumSum += element(v1(offset), v2(offset))
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
          if (offset < v1.length && offset < v2.length) {
            cumSum += element(v1(offset), v2(offset), sweights.valueAt(offset))
          }
          offset += 1
        }
      } else if (v1.isInstanceOf[SparseFeatureVector] && v2.isInstanceOf[SparseFeatureVector]) {
        //dense weights, sparse vectors
        log.trace("compute distance with dense weights and sparse vectors")

        val sv1 = v1.asInstanceOf[SparseFeatureVector]
        val sv2 = v2.asInstanceOf[SparseFeatureVector]

        var offset = 0
        while (offset < math.max(sv1.activeSize, sv2.activeSize)) {
          if (offset < sv1.activeSize && offset < sv2.activeSize) {
            cumSum += element(sv1.valueAt(offset), sv2.valueAt(offset), weights.get(offset))
          } else if (offset < sv1.activeSize) {
            cumSum += element(sv1.valueAt(offset), 0.0, weights.get(offset))
          } else if (offset < sv2.activeSize) {
            cumSum += element(0.0, sv2.valueAt(offset), weights.get(offset))
          }
          offset += 1
        }
      } else {
        //dense weights, dense vectors
        log.trace("compute distance with dense weights and dense vectors")

        var offset = 0
        while (offset < v1.length && offset < v1.length && offset < v2.length) {
          cumSum += element(v1(offset), v2(offset), weights.get(offset))
          offset += 1
        }
      }
    }

    normalize(cumSum)
  }


  /**
    * Element-wise computation.
    *
    * @param v1 value 1
    * @param v2 value 2
    * @param w  weight
    * @return
    */
  def element(v1: VectorBase, v2: VectorBase, w: VectorBase = 1.0): Distance

  /**
    * Normalization after summing up the element-wise distances.
    *
    * @param sum cumulative sum
    * @return
    */
  def normalize(sum: Distance): Distance = sum
}
