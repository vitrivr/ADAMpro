package org.vitrivr.adampro.data.index.structures.va.marks

import breeze.linalg.{DenseVector, max, min}
import org.vitrivr.adampro.data.datatypes.vector.Vector
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.data.index.IndexingTaskTuple
import org.vitrivr.adampro.data.index.structures.va.VAIndex.Marks
import org.vitrivr.adampro.utils.Logging

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  *
  * equidistant marks generator: all marks have the same distance from each other; for this the minimum and the maximum value per dimension
  * is considered and the space along each dimension is split over this range in equi-distant cells
  */
private[va] object EquidistantMarksGenerator extends MarksGenerator with Serializable with Logging {

  /**
    *
    * @param samples  training samples
    * @param maxMarks maximal number of marks
    * @return
    */
  private[va] def getMarks(samples: Seq[IndexingTaskTuple], maxMarks: Seq[Int]): Marks = {
    log.trace("get equidistant marks for VA-File")
    val min = getMin(samples.map(_.ap_indexable)).toArray
    val max = getMax(samples.map(_.ap_indexable)).toArray

    (min zip max).zipWithIndex.map { case (minmax, index) => Seq.tabulate(maxMarks(index) - 1)(_ * (minmax._2 - minmax._1) / (maxMarks(index) - 1).toFloat + minmax._1).toList ++ Seq(minmax._2) }
  }

  /**
    *
    * @param data
    * @return
    */
  private def getMin(data: Seq[MathVector]): MathVector = {
    val dimensionality = data.head.size
    val base : MathVector = DenseVector.fill(dimensionality)(Vector.maxValue)

    data.foldLeft(base)((baseV, newV) => min(baseV, newV))
  }

  /**
    *
    * @param data
    * @return
    */
  private def getMax(data: Seq[MathVector]): MathVector = {
    val dimensionality = data.head.size
    val base : MathVector = DenseVector.fill(dimensionality)(Vector.minValue)

    data.foldLeft(base)((baseV, newV) => max(baseV, newV))
  }
}
