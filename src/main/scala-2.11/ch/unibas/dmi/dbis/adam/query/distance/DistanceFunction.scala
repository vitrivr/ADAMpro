package ch.unibas.dmi.dbis.adam.query.distance

import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.Distance._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait DistanceFunction {
  /**
   *
   * @param v1
   * @param v2
   * @return
   */
  def apply(v1: WorkingVector, v2: WorkingVector): Distance

  /**
   *
   * @param v1
   * @param v2
   * @return
   */
  def apply(v1: StoredVector, v2: StoredVector): Distance

  /**
   *
   * @param v1
   * @param v2
   * @return
   */
  def apply(v1: VectorBase, v2: VectorBase): Distance
}
