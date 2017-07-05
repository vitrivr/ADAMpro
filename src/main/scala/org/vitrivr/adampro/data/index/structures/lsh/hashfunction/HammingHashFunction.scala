package org.vitrivr.adampro.data.index.structures.lsh.hashfunction

import org.vitrivr.adampro.data.datatypes.vector.Vector.{MathVector}

import scala.util.Random

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * April 2017
  */
@SerialVersionUID(100L)
class HammingHashFunction(j: Int) extends LSHashFunction with Serializable {
  /**
    *
    * @param v
    * @return
    */
  def hash(v: MathVector): Int = v.apply(j).toInt
}

object HammingHashFunction {
  def withDimension(d: Int) = new HammingHashFunction(Random.nextInt(d))
}