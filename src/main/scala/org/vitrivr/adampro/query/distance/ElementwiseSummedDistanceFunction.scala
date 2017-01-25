package org.vitrivr.adampro.query.distance

import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.query.distance.Distance.Distance
import org.vitrivr.adampro.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
trait ElementwiseSummedDistanceFunction extends DistanceFunction with Logging with Serializable {
  override def apply(v1_q: MathVector, v2: MathVector, weights: Option[MathVector]): Distance = {
    var cumSum = 0.0

    //computing sum
    if (weights.isEmpty) {
      //un-weighted
      if (v1_q.isInstanceOf[SparseMathVector] && v2.isInstanceOf[SparseMathVector]) {
        //sparse vectors
        val sv1_q = v1_q.asInstanceOf[SparseMathVector]
        val sv2 = v2.asInstanceOf[SparseMathVector]
        var offset = 0

        while (offset < math.max(sv1_q.activeSize, sv2.activeSize)) {
          if (offset < sv1_q.activeSize && offset < sv2.activeSize) {
            cumSum += element(sv1_q.valueAt(offset), sv2.valueAt(offset))
          } else if (offset < sv1_q.activeSize) {
            cumSum += element(sv1_q.valueAt(offset), 0.0)
          } else if (offset < sv2.activeSize) {
            cumSum += element(0.0, sv2.valueAt(offset))
          }
          offset += 1
        }
      } else {
        //dense vectors
        var offset = 0
        while (offset < math.min(v1_q.length, v2.length)) {
          cumSum += element(v1_q(offset), v2(offset))
          offset += 1
        }
      }
    } else {
      //weighted
      if (weights.get.isInstanceOf[SparseMathVector]) {
        //sparse weights

        val sweights = weights.get.asInstanceOf[SparseMathVector]

        var offset = 0
        while (offset < sweights.activeSize) {
          if (offset < v1_q.length && offset < v2.length) {
            cumSum += element(v1_q(offset), v2(offset), sweights.valueAt(offset))
          }
          offset += 1
        }
      } else if (v1_q.isInstanceOf[SparseMathVector] && v2.isInstanceOf[SparseMathVector]) {
        //dense weights, sparse vectors
        val sv1_q = v1_q.asInstanceOf[SparseMathVector]
        val sv2 = v2.asInstanceOf[SparseMathVector]

        var offset = 0
        while (offset < math.max(sv1_q.activeSize, sv2.activeSize)) {
          if (offset < sv1_q.activeSize && offset < sv2.activeSize) {
            cumSum += element(sv1_q.valueAt(offset), sv2.valueAt(offset), weights.get(offset))
          } else if (offset < sv1_q.activeSize) {
            cumSum += element(sv1_q.valueAt(offset), 0.0, weights.get(offset))
          } else if (offset < sv2.activeSize) {
            cumSum += element(0.0, sv2.valueAt(offset), weights.get(offset))
          }
          offset += 1
        }
      } else {
        //dense weights, dense vectors
        var offset = 0
        while (offset < v1_q.length && offset < v1_q.length && offset < v2.length) {
          cumSum += element(v1_q(offset), v2(offset), weights.get(offset))
          offset += 1
        }
      }
    }

    normalize(cumSum)
  }


  /**
    * Element-wise computation.
    *
    * @param v1_q value 1 (from query vector)
    * @param v2 value 2
    * @param w  weight
    * @return
    */
  @inline def element(v1_q: VectorBase, v2: VectorBase, w: VectorBase = 1.0): Distance

  /**
    * Normalization after summing up the element-wise distances.
    *
    * @param sum cumulative sum
    * @return
    */
  @inline def normalize(sum: Distance): Distance = sum
}
