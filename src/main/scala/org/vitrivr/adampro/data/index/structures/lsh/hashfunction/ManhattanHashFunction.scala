package org.vitrivr.adampro.data.index.structures.lsh.hashfunction

import breeze.linalg.DenseVector
import org.vitrivr.adampro.data.datatypes.vector.Vector
import org.vitrivr.adampro.data.datatypes.vector.Vector._

import scala.util.Random

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
@SerialVersionUID(100L)
class ManhattanHashFunction(w: VectorBase, offset: VectorBase, proj: MathVector, m: Int) extends LSHashFunction with Serializable {
  /**
    *
    * @param d
    * @param w
    * @param m
    */
  def this(d: Int, w: VectorBase, m: Int) {
    this(w, w * Vector.nextRandom(), Vector.conv_draw2vec(CauchyDistribution.getNext(d).map(Vector.conv_double2vb)), m)
  }


  /**
    *
    * @param v
    * @return
    */
  def hash(v: MathVector): Int = (math.round((v dot proj : VectorBase) + offset / w) % m).toInt
}


private object CauchyDistribution {
  private val dist = new breeze.stats.distributions.CauchyDistribution(0, 1)

  def getNext = dist.sample()

  def getNext(n : Int) = dist.sample(n)
}