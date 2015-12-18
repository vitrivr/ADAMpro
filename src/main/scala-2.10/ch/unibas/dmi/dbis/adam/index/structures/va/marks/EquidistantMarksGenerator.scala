package ch.unibas.dmi.dbis.adam.index.structures.va.marks

import breeze.linalg.{min, max}
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.index.IndexerTuple
import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex.Marks

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
private[va] object EquidistantMarksGenerator extends MarksGenerator with Serializable {

  /**
   *
   * @param samples
   * @param maxMarks
   * @return
   */
  private[va] def getMarks(samples : Array[IndexerTuple], maxMarks : Seq[Int]) : Marks = {
    val dimensionality = maxMarks.length

    val min = getMin(samples.map(_.value))
    val max = getMax(samples.map(_.value))

    (min zip max).zipWithIndex.map { case (minmax, index) => Seq.tabulate(maxMarks(index))(_ * (minmax._2 - minmax._1) / maxMarks(index).toFloat + minmax._1).toList }
  }

  /**
   *
   * @param data
   * @return
   */
  private def getMin(data : Array[FeatureVector]) : FeatureVector = {
    val dimensionality = data.head.size
    val base : FeatureVector = Seq.fill(dimensionality)(Float.MaxValue)

    data.foldLeft(base)((baseV, newV) =>  min(baseV, newV))
  }

  /**
   *
   * @param data
   * @return
   */
  private def getMax(data : Array[FeatureVector]) : FeatureVector = {
    val dimensionality = data.head.size
    val base : FeatureVector = Seq.fill(dimensionality)(Float.MinValue)

    data.foldLeft(base)((baseV, newV) =>  max(baseV, newV))
  }
}
