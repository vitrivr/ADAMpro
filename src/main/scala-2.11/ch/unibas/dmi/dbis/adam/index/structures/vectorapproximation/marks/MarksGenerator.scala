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
private[vectorapproximation] trait MarksGenerator extends Serializable {
  /**
   * 
   * @param sample
   * @param maxMarks
   * @return
   */
  private[vectorapproximation] def getMarks(sample: RDD[IndexerTuple[WorkingVector]], maxMarks: Seq[Int]): Marks

  /**
   *
   * @param sample
   * @param maxMarks
   * @return
   */
  private[vectorapproximation] def getMarks(sample: RDD[IndexerTuple[WorkingVector]], maxMarks: Int): Marks = {
    val dimensionality = sample.first.value.length
    getMarks(sample, Seq.fill(dimensionality)(maxMarks))
  }
}
