package ch.unibas.dmi.dbis.adam.index.structures.va.marks

import breeze.linalg.{max, min}
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.index.IndexingTaskTuple
import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex.Marks
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer

/**
  *
  */
private[va] object EquifrequentMarksGenerator extends MarksGenerator with Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  val SAMPLING_FREQUENCY = 10000

  /**
    *
    * @param samples  training samples
    * @param maxMarks maximal number of marks
    * @return
    */
  private[va] def getMarks(samples: Array[IndexingTaskTuple[_]], maxMarks: Seq[Int]): Marks = {
    log.debug("get equifrequent marks for VA-File")

    val sampleSize = samples.length

    val min = getMin(samples.map(_.feature))
    val max = getMax(samples.map(_.feature))

    val dimensionality = min.length

    val dimData = (0 until dimensionality).map(dim => Distribution(min(dim), max(dim), SAMPLING_FREQUENCY))

    samples.foreach { sample =>
      var i = 0
      while (i < dimensionality) {
        dimData(i).add(sample.feature(i))
        i += 1
      }
    }

    (0 until dimensionality).map({ dim =>
      val hist = dimData(dim).histogram

      var marks = new Array[Float](maxMarks(dim) - 1)
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

      marks(0) = min(dim)

      marks.toSeq
    })
  }

  /**
    *
    * @param data
    * @return
    */
  private def getMin(data: Array[FeatureVector]): FeatureVector = {
    val dimensionality = data.head.size
    val base: FeatureVector = Seq.fill(dimensionality)(Float.MaxValue)

    data.foldLeft(base)((baseV, newV) => min(baseV, newV))
  }

  /**
    *
    * @param data
    * @return
    */
  private def getMax(data: Array[FeatureVector]): FeatureVector = {
    val dimensionality = data.head.size
    val base: FeatureVector = Seq.fill(dimensionality)(Float.MinValue)

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
    def histogram : IndexedSeq[Int] = {
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
