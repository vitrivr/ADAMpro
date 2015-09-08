package ch.unibas.dmi.dbis.adam.query.distance

import ch.unibas.dmi.dbis.adam.data.types.Feature.{WorkingVector, VectorBase, StoredVector}
import ch.unibas.dmi.dbis.adam.query.distance.Distance.{Distance, _}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
case class NormBasedDistanceFunction(n : Int) extends DistanceFunction with Serializable {
  /**
   *
   * @param v1
   * @param v2
   * @return
   */
  def apply(v1: WorkingVector, v2: WorkingVector): Distance = {
    n match {
      case 1 => v1.valuesIterator.zip(v2.valuesIterator).map { case (vv1: Float, vv2: Float) => math.abs(vv1 - vv2) }.reduce(_ + _)
      case _ => v1.valuesIterator.zip(v2.valuesIterator).map { case (vv1: Float, vv2: Float) => math.pow(math.abs(vv1 - vv2), n) }.reduce(_ + _)
    }
  }

  /**
   *
   * @param v1
   * @param v2
   * @return
   */
  def apply(v1: StoredVector, v2: StoredVector): Distance = {
    n match {
      case 1 => v1.iterator.zip(v2.iterator).map { case (vv1: Float, vv2: Float) => math.abs(vv1 - vv2) }.reduce(_ + _)
      case _ => v1.iterator.zip(v2.iterator).map { case (vv1: Float, vv2: Float) => math.pow(math.abs(vv1 - vv2), n) }.reduce(_ + _)
    }
  }

  /**
   *
   * @param v1
   * @param v2
   * @return
   */
  def apply(v1: VectorBase, v2: VectorBase): Distance = {
    math.pow(math.abs(v1 - v2), n)
  }
}