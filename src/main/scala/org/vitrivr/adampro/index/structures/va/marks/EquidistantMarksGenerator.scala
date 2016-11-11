package org.vitrivr.adampro.index.structures.va.marks

import breeze.linalg.{max, min}
import org.vitrivr.adampro.datatypes.feature.Feature._
import org.vitrivr.adampro.index.IndexingTaskTuple
import org.vitrivr.adampro.index.structures.va.VAIndex.Marks
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
  private[va] def getMarks(samples: Array[IndexingTaskTuple[_]], maxMarks: Seq[Int]): Marks = {
    log.debug("get equidistant marks for VA-File")
    val dimensionality = maxMarks.length

    val min = getMin(samples.map(_.feature))
    val max = getMax(samples.map(_.feature))

    (min zip max).zipWithIndex.map { case (minmax, index) => Seq.tabulate(maxMarks(index))(_ * (minmax._2 - minmax._1) / maxMarks(index).toFloat + minmax._1).toList }
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
}
