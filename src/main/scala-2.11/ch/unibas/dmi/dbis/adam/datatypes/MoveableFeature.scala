package ch.unibas.dmi.dbis.adam.datatypes

import breeze.linalg.DenseVector
import Feature.{VectorBase, WorkingVector}
import breeze.linalg.{Matrix, Vector, _}

import scala.util.Random

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class MoveableFeature(val v: WorkingVector) {
  /**
   *
   * @param radius
   * @return
   */
  def move(radius : Double) : WorkingVector = move(radius.toFloat)

  /**
   *
   * @param radius
   * @return
   */
  def move(radius : Float) : WorkingVector = {
      val diff = DenseVector.fill(v.length)(radius - 2 * radius * Random.nextFloat)
      v + diff
  }

  /**
   *
   * @param radius
   * @return
   */
  def move(radius : Vector[VectorBase]) : WorkingVector = {
    val diff = radius - 2 * Random.nextFloat * radius
    v + diff
  }
}

object MovableFeature {
  implicit def conv_feature2MovableFeature(v: WorkingVector) = new MoveableFeature(v)
}