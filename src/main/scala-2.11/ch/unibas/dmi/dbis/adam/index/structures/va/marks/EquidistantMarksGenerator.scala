package ch.unibas.dmi.dbis.adam.index.structures.va.marks

import breeze.linalg.{max, min}
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import Feature._
import ch.unibas.dmi.dbis.adam.index.IndexerTuple
import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex.Marks
import org.apache.spark.rdd.RDD

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
  private[va] def getMarks(samples : RDD[IndexerTuple], maxMarks : Seq[Int]) : Marks = {
    val dimensionality = maxMarks.length

    val min = getMin(samples.map(_.value))
    val max = getMax(samples.map(_.value))

    (min zip max).zipWithIndex.map { case (minmax, index) => Seq.tabulate(maxMarks(index))(_ * (minmax._2 - minmax._1) / maxMarks(index).toFloat + minmax._1).toList }
  }

  private def getMin(data : RDD[FeatureVector]) : FeatureVector = data.treeReduce { case (baseV, newV) =>  min(baseV, newV)  }
  private def getMax(data : RDD[FeatureVector]) : FeatureVector = data.treeReduce { case (baseV, newV) =>  max(baseV, newV)  }
}
