package org.vitrivr.adampro.data.index.structures.lsh.hashfunction

import org.vitrivr.adampro.data.datatypes.vector.Vector
import org.vitrivr.adampro.data.datatypes.vector.Vector._

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
@SerialVersionUID(100L)
class EuclideanHashFunction(w: VectorBase, offset: VectorBase, proj: MathVector, m: Int) extends LSHashFunction with Serializable {
  /**
    *
    * @param d
    * @param w
    * @param m
    */
  def this(d: Int, w: VectorBase, m: Int) {
    this(w, w * Vector.nextRandom(), Vector.conv_draw2vec(GaussianDistribution.getNext(d).map(Vector.conv_double2vb)), m)
  }


  /**
    *
    * @param v
    * @return
    */
  def hash(v: MathVector): Int = (math.round((v dot proj : VectorBase) + offset / w) % m).toInt
}


private object GaussianDistribution {
  private val dist = new breeze.stats.distributions.Gaussian(0, 1)

  def getNext = dist.sample()

  def getNext(n: Int) = dist.sample(n)
}