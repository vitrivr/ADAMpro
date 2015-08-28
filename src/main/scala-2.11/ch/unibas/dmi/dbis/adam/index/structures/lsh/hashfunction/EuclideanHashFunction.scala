package ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction

import breeze.linalg.DenseVector
import ch.unibas.dmi.dbis.adam.data.types.Feature.WorkingVector

import scala.util.Random

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class EuclideanHashFunction(w: Float, offset: Float, proj: WorkingVector, m: Int) extends LSHashFunction with Serializable {
  /**
   *
   * @param d
   * @param w
   * @param m
   */
  def this(d: Int, w: Float, m: Int) {
    this(w, w * Random.nextFloat(), DenseVector.fill(d)(Random.nextGaussian().toFloat), m: Int)
  }

  /**
   *
   * @param v
   * @return
   */
  def hash(v: WorkingVector): Int = {
    math.round(((v dot proj): Float) + offset / w.toFloat) % m
  }
}


object EuclideanHashFunction {
  /**
   *
   * @param d
   * @param w
   * @param m
   * @return
   */
  def apply(d: Int, w: Float, m: Int) = {
    new EuclideanHashFunction(d, w, m)
  }
}
