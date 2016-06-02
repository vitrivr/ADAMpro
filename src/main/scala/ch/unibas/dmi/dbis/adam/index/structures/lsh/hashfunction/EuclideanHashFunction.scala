package ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction

import breeze.linalg.DenseVector
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector

import scala.util.Random

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
@SerialVersionUID(100L)
class EuclideanHashFunction(w: Float, offset: Float, proj: FeatureVector, m: Int) extends LSHashFunction with Serializable {
  /**
    *
    * @param d
    * @param w
    * @param m
    */
  def this(d: Int, w: Float, m: Int) {
    this(w, w * Random.nextFloat(), DenseVector(GaussianDistribution.getNext(d).map(_.toFloat).toArray), m: Int)
  }


  /**
    *
    * @param v
    * @return
    */
  def hash(v: FeatureVector): Int = math.round((v dot proj: Float) + offset / w.toFloat) % m
}


private object GaussianDistribution {
  private val dist = new breeze.stats.distributions.Gaussian(0, 1)

  def getNext = dist.sample()

  def getNext(n: Int) = dist.sample(n)
}