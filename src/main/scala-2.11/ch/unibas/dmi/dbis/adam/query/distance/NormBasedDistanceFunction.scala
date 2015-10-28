package ch.unibas.dmi.dbis.adam.query.distance

import ch.unibas.dmi.dbis.adam.datatypes.Feature.{VectorBase, WorkingVector}
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
    var sum : Float = 0

    var i = 0
    while(i < math.min(v1.length, v2.length)){
      sum += math.pow(math.abs(v1(i) - v2(i)), n)
      i += 1
    }

    sum
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