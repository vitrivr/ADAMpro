package ch.unibas.dmi.dbis.adam.index.structures.va.marks

import ch.unibas.dmi.dbis.adam.index.IndexerTuple
import ch.unibas.dmi.dbis.adam.index.structures.va.VectorApproximationIndex.Marks
import org.apache.spark.rdd.RDD


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
private[va] trait MarksGenerator extends Serializable {
  /**
   * 
   * @param sample
   * @param maxMarks
   * @return
   */
  private[va] def getMarks(sample: RDD[IndexerTuple], maxMarks: Seq[Int]): Marks

  /**
   *
   * @param sample
   * @param maxMarks
   * @return
   */
  private[va] def getMarks(sample: RDD[IndexerTuple], maxMarks: Int): Marks = {
    val dimensionality = sample.first.value.length
    getMarks(sample, Seq.fill(dimensionality)(maxMarks))
  }
}
