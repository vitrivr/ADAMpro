package org.vitrivr.adampro.utils.ml

import breeze.linalg.DenseVector
import org.vitrivr.adampro.utils.Logging

import scala.util.Random


case class TrainingSample(f: DenseVector[Double], time: Double)

/**
  * PegasosSVM
  * <p>
  * see S. Shalev-Shwartz, Y. Singer, N. Srebro, A. Cotter (2011). Pegasos: Primal Estimated sub-GrAdient SOlver for SVM.
  *
  *
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
class PegasosSVM(private val dims: Int, private val lambda: Double = 100, private val batchSize: Int = 5, private val epsilon: Double = 0.01) extends Logging with Serializable {
  private val MAX_ITER = 5000

  private var w = DenseVector.zeros[Double](dims)
  private var b = 2 * Random.nextDouble() - 1
  private var t = 1


  /**
    *
    * @param data
    */
  def train(data: Seq[TrainingSample]) {
    if (data.length > batchSize) {
      Random.shuffle(data).grouped(batchSize).foreach {
        train(_)
      }
    } else {
      //do training with mini-batch

      var batchW = DenseVector.zeros[Double](dims)
      var batchB = b

      data.foreach { datum =>
        val x = datum.f //vector
        val y = datum.time //measurement

        //epsilon insensitive loss
        val loss = math.abs((w.t * x + b) - y) - epsilon

        if (loss > 0) {
          batchW += x * y
          batchB += y
        }
      }

      w = w * (1.0 - 1.0 / t) + batchW * (1.0 / (data.length * lambda * t))
      b = b * (1.0 - 1.0 / t) + batchB * (1.0 / (data.length * lambda * t))
      t += 1
    }
  }

  /**
    *
    * @param f
    * @return
    */
  def test(f: DenseVector[Double]): Double = w.t * f + b
}