package ch.unibas.dmi.dbis.adam.index.structures.va.marks

import breeze.linalg.{max, min}
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.index.IndexingTaskTuple
import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex.Marks
import org.apache.log4j.Logger

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
private[va] object EquidistantMarksGenerator extends MarksGenerator with Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  /**
   *
   * @param samples training samples
   * @param maxMarks maximal number of marks
   * @return
   */
  private[va] def getMarks(samples : Array[IndexingTaskTuple[_]], maxMarks : Seq[Int]) : Marks = {
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
