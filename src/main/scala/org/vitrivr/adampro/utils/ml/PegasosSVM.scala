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
@SerialVersionUID(100L)
class PegasosSVM(private val dims: Int, private val lambda: Double = 1, private val batchSize: Int = 5, private val epsilon: Double = 100) extends Logging with Serializable {
  private val MAX_ITER = 5000

  private var w = DenseVector.zeros[Double](dims)
  private var t = 1

  /**
    *
    * @param data
    */
  def train(data: Seq[TrainingSample]) {
    if (data.length > batchSize) {
      var oldMSE = Double.MaxValue
      var newMSE = Double.MaxValue

      do {
        val shuffledData = Random.shuffle(data)
        val (trainData, testData) = shuffledData.splitAt((0.8 * data.length).toInt)

        trainData.grouped(batchSize).foreach {
          train(_)
        }

        oldMSE = newMSE
        newMSE = mse(testData)

      } while ((math.abs(newMSE - oldMSE) > 0.001 * oldMSE && t < 100) || t < MAX_ITER)
    } else {
      //do training with mini-batch

      var batchW = DenseVector.zeros[Double](dims)

      data.foreach { datum =>
        val x = datum.f //vector
        val y = datum.time //measurement

        val loss = math.max(0, math.abs(y - (w.t * x)) - epsilon)

        batchW += x * loss
      }

      w = w * (1.0 - 1.0 / t) + batchW * (1.0 / (data.length * t * lambda))

      t += 1
    }
  }

  private def mse(sample: Seq[TrainingSample]): Double = {
    sample.map { datum =>
      val x = datum.f //vector
      val y = datum.time //measurement

      val tested = test(x)

      (y - tested) * (y - tested)
    }.sum / sample.size.toDouble
  }

  /**
    *
    * @param f
    * @return
    */
  def test(f: DenseVector[Double]): Double = w.t * f
}