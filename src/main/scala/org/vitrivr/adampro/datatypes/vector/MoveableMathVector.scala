package org.vitrivr.adampro.datatypes.vector

import org.vitrivr.adampro.datatypes.vector.Vector._

/**
  * adamtwo
  *
  * Implements a vector that can be moved to some small extent given a radius.
  *
  * Ivan Giangreco
  * September 2015
  */
class MoveableMathVector(val v: MathVector) {
  /**
    * Moves vector randomly according to the given radius (same radius for all dimensions).
    *
    * @param radius radius which is the maximum used for moving vector around
    * @return
    */
  def move(radius: VectorBase): MathVector = {
    val diff : MathVector = Vector.conv_draw2vec(Seq.fill(v.length)(radius - 2 * Vector.nextRandom() * radius))
    v + diff
  }

  /**
    * Moves vector randomly according to the given radius (different radius for each dimension).
    *
    * @param radius feature vector which contains a radius in each dimension which is the maximum used for moving vector around
    * @return
    */
  def move(radius: MathVector): MathVector = {
    val diff : MathVector = radius - 2 * Vector.nextRandom() * radius
    v + diff
  }
}

object MovableFeature {
  implicit def conv_math2mov(v: MathVector): MoveableMathVector = new MoveableMathVector(v)
}