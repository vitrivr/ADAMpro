package org.vitrivr.adampro.data.index.structures.va.marks

import breeze.linalg.{max, min}
import org.vitrivr.adampro.data.datatypes.vector.Vector
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.data.index.IndexingTaskTuple
import org.vitrivr.adampro.data.index.structures.va.VAIndex.Marks
import org.vitrivr.adampro.utils.Logging

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * September 2015
  *
  * equifrequent marks generator: all cells have about the same number of points; for this we use training data, build a histogram
  * and try to fit approximately the same number of points in each cell; the space along each dimension is split into cells with
  * equi-frequent points
  */
private[va] object EquifrequentMarksGenerator extends MarksGenerator with Serializable with Logging {

  val SAMPLING_FREQUENCY = 10000 //number of buckets for the histogram (not the number of marks!)

  /**
    *
    * @param samples  training samples
    * @param maxMarks maximal number of marks
    * @return
    */
  private[va] def getMarks(samples: Seq[IndexingTaskTuple], maxMarks: Seq[Int]): Marks = {
    log.debug("get equifrequent marks for VA-File")

    val sampleSize = samples.length

    val min = getMin(samples.map(_.ap_indexable))
    val max = getMax(samples.map(_.ap_indexable))

    val dimensionality = min.length

    val dimData = (0 until dimensionality).map(dim => Distribution(min(dim), max(dim), SAMPLING_FREQUENCY))

    samples.foreach { sample =>
      var i = 0
      while (i < dimensionality) {
        dimData(i).add(sample.ap_indexable(i))
        i += 1
      }
    }

    (0 until dimensionality).map({ dim =>
      if (maxMarks(dim) > 2) {
        val hist = dimData(dim).histogram

        val marks = new Array[VectorBase](maxMarks(dim) - 1)
        var k = 0
        var sum = 0
        for (j <- 1 until (maxMarks(dim) - 1)) {
          var n = (hist.sum - sum) / (maxMarks(dim) - j)

          while ((j % 2 == 1 && k < hist.length && n > 0) || (j % 2 == 0 && k < hist.length && n > hist(k))) {
            sum += hist(k)
            n -= hist(k)
            k += 1
          }

          marks(j) = min(dim) + k.toFloat * (max(dim) - min(dim)) / SAMPLING_FREQUENCY.toFloat
        }

        marks.toSeq
      } else {
        Seq(min(dim), max(dim))
      }
    })
  }

  /**
    *
    * @param data
    * @return
    */
  private def getMin(data: Seq[MathVector]): MathVector = {
    val dimensionality = data.head.size
    val base: MathVector = Vector.conv_draw2vec(Seq.fill(dimensionality)(Vector.maxValue))

    data.foldLeft(base)((baseV, newV) => min(baseV, newV))
  }

  /**
    *
    * @param data
    * @return
    */
  private def getMax(data: Seq[MathVector]): MathVector = {
    val dimensionality = data.head.size
    val base: MathVector = Vector.conv_draw2vec(Seq.fill(dimensionality)(Vector.minValue))

    data.foldLeft(base)((baseV, newV) => max(baseV, newV))
  }

  /**
    *
    * @param min
    * @param max
    * @param sampling_frequency
    */
  private case class Distribution(min: VectorBase, max: VectorBase, sampling_frequency: Int) {
    val data = new ListBuffer[VectorBase]()

    /**
      *
      * @param item
      */
    def add(item: VectorBase): Unit = data += item

    /**
      *
      * @return
      */
    def histogram: IndexedSeq[Int] = {
      val counts = data
        .map(x => {
          var j = (((x - min) / (max - min)) * sampling_frequency).floor.toInt
          if (j < 0) {
            j = 0
          }
          if (j >= sampling_frequency) {
            j = sampling_frequency - 1
          }

          j
        })
        .groupBy(x => x).map { case (key, value) => (key, value.size) }

      (0 until sampling_frequency).map(counts.getOrElse(_, 0))
    }
  }

}
