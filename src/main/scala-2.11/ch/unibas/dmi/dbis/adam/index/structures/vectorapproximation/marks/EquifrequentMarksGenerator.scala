package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.marks

import ch.unibas.dmi.dbis.adam.data.IndexTuple
import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndexer.Marks
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.IterableExtension._

import org.apache.spark.rdd.RDD

/**
 * 
 */
object EquifrequentMarksGenerator extends MarksGenerator with Serializable {
  val HistogramBucketCount = 500

  /**
   *
   * @param sample
   * @param maxMarks
   * @return
   */
  def getMarksForSample(sample: RDD[IndexTuple[WorkingVector]], maxMarks: Int): Marks = {
    val sampleSize = sample.count
    val dims = sample.first.value.length

    val hists = (0 until dims).map(dim => sample.map { _.value(dim).toDouble }).map { data => data.histogram(HistogramBucketCount) }
    val counts = hists.map { hist => getCounts(hist._2, sampleSize, maxMarks) }

    val result = (hists zip counts).map {
      case (hist, count) =>
        val min = hist._1(0).toFloat
        val max = hist._1(HistogramBucketCount - 1).toFloat

        val interpolated = count.map(_.toFloat).map(_ * (max - min) / sampleSize.toFloat + min).toList

        min +: interpolated :+ max
    }

    result
  }

  /**
   *
   * @param hist
   * @param sampleSize
   * @param maxMarks
   * @return
   */
  def getCounts(hist: Seq[Long], sampleSize: Long, maxMarks: Int) = {
    (1 until (maxMarks - 1)).map { j =>
      val nppart = sampleSize * j / (maxMarks - 1)
      val countSum = hist.foldLeftWhileCounting(0.toLong)(_ <= nppart) { case (acc, bucket) => acc + bucket }

      countSum._1
    }
  }

}
