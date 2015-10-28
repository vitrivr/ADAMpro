package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.marks

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
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
  private[vectorapproximation] def getMarks(sample : RDD[IndexerTuple[WorkingVector]], maxMarks : Seq[Int]) : Marks = {
    val dimensionality = maxMarks.length

    val min = getMin(sample.map(_.value), dimensionality)
    val max = getMax(sample.map(_.value), dimensionality)

    (min zip max).zipWithIndex.map { case (minmax, index) => Seq.tabulate(maxMarks(index))(_ * (minmax._2 - minmax._1) / maxMarks(index).toFloat + minmax._1).toList }
  }

  /**
   *
   * @param data
   * @param dimensionality
   * @return
   */
  private def getMin(data : RDD[WorkingVector], dimensionality : Int) : WorkingVector = {
    val base = Seq.fill(dimensionality)(Float.MaxValue)
    data.treeReduce{case(baseV, newV) => baseV.zip(newV).map{case (b,v) => math.min(b,v)}}
  }

  /**
   *
   * @param data
   * @param dimensionality
   * @return
   */
  private def getMax(data : RDD[WorkingVector], dimensionality : Int) : WorkingVector = {
    val base = Seq.fill(dimensionality)(Float.MinValue)
    data.treeReduce{case(baseV, newV) => baseV.zip(newV).map{case (b,v) => math.max(b,v)}}
  }
}
