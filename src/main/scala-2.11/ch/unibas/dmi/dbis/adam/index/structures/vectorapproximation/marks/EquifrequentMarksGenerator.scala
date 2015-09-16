package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.marks

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.index.IndexerTuple
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndex.Marks
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * 
 */
private[vectorapproximation] object EquifrequentMarksGenerator extends MarksGenerator with Serializable {
  val SamplingFrequency = 500

  /**
   *
   * @param samples
   * @param maxMarks
   * @return
   */
  private[vectorapproximation] def getMarks(samples : RDD[IndexerTuple[WorkingVector]], maxMarks : Int) : Marks = {
    val sampleSize = samples.count
    val dims = samples.first.value.length

    val min = treeReduceData(samples.map(_.value), dims, math.min)
    val max = treeReduceData(samples.map(_.value), dims, math.max)

    val result = (0 until dims).map(dim => Distribution(min(dim), max(dim), SamplingFrequency))

    samples.collect.foreach { sample =>
      var i = 0
      while (i < dims){
        result(i).add(sample.value(i))
        i += 1
      }
    }

    (0 until dims).map({ dim =>
      val counts = result(dim).getCounts(maxMarks)

      val interpolated = counts.map(_.toFloat).map(_ * (max(dim) - min(dim)) / sampleSize.toFloat + min(dim))

      min(dim) +: interpolated :+ max(dim)
    })
  }

  /**
   *
   * @param data
   * @param dimensionality
   * @return
   */
  private def treeReduceData(data : RDD[StoredVector], dimensionality : Int, f : (VectorBase, VectorBase) => Float) : StoredVector = {
    val base = Seq.fill(dimensionality)(Float.MaxValue)
    data.treeReduce{case(baseV, newV) => baseV.zip(newV).map{case (b,v) => f(b,v)}}
  }

  /**
   *
   * @param min
   * @param max
   * @param sampling_frequency
   */
  private case class Distribution(min: VectorBase, max: VectorBase, sampling_frequency: Int) {
    val binWidth = (max - min) / sampling_frequency
    val bounds = (1 to sampling_frequency).map(x => min + binWidth * x).toList
    val data = new ListBuffer[VectorBase]()

    /**
     *
     * @param item
     */
    def add(item : VectorBase): Unit ={
      data += item
    }

    /**
     *
     * @return
     */
    def getHistogram = buildHistogram(bounds, data.toList)


    /**
     *
     * @param bounds
     * @param data
     * @return
     */
    private def buildHistogram(bounds: List[VectorBase], data: List[VectorBase]): List[List[VectorBase]] = {
      bounds match {
        case h :: Nil =>
          List(data)
        case h :: t =>
          val (l, r) = data.partition(_ <= h); l :: buildHistogram(t, r)
        case Nil => List(data)
      }
    }

    /**
     *
     * @param maxMarks
     * @return
     */
    def getCounts(maxMarks: Int) = {
      val sampleSize = data.length
      val hist = getHistogram

      (1 until (maxMarks - 1)).map { j =>
        val nppart = sampleSize * j / (maxMarks - 1)

        val countSum = hist.foldLeftWhileCounting(0.toLong)(_ <= nppart) { case (acc, bucket) => acc + bucket.length }
        val res =  countSum._2
        res
      }
    }
  }

}
