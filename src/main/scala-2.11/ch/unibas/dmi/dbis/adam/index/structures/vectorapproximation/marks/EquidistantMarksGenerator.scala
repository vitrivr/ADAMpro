package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.marks

import ch.unibas.dmi.dbis.adam.datatypes.Feature
import Feature._
import ch.unibas.dmi.dbis.adam.index.IndexerTuple
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndex.Marks
import org.apache.spark.rdd.RDD

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
private[vectorapproximation] object EquidistantMarksGenerator extends MarksGenerator with Serializable {

  /**
   *
   * @param sample
   * @param maxMarks
   * @return
   */
  private[vectorapproximation] def getMarks(sample : RDD[IndexerTuple[WorkingVector]], maxMarks : Int) : Marks = {
    val dimensionality = sample.first.value.length

    val min = getMin(sample.map(_.value), dimensionality)
    val max = getMax(sample.map(_.value), dimensionality)

    (min zip max).map { case (min, max) => Seq.tabulate(maxMarks)(_ * (max - min) / maxMarks.toFloat + min).map(_.toFloat).toList }
  }

  /**
   *
   * @param data
   * @param dimensionality
   * @return
   */
  private def getMin(data : RDD[StoredVector], dimensionality : Int) : StoredVector = {
    val base = Seq.fill(dimensionality)(Float.MaxValue)
    data.treeReduce{case(baseV, newV) => baseV.zip(newV).map{case (b,v) => math.min(b,v)}}
  }

  /**
   *
   * @param data
   * @param dimensionality
   * @return
   */
  private def getMax(data : RDD[StoredVector], dimensionality : Int) : StoredVector = {
    val base = Seq.fill(dimensionality)(Float.MinValue)
    data.treeReduce{case(baseV, newV) => baseV.zip(newV).map{case (b,v) => math.max(b,v)}}
  }
}
