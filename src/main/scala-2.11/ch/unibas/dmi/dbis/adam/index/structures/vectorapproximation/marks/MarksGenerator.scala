package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.marks

import ch.unibas.dmi.dbis.adam.data.IndexTuple
import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndexer.Marks
import org.apache.spark.adam.ADAMSamplingUtils
import org.apache.spark.rdd.RDD


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait MarksGenerator extends Serializable {

  /**
   *
   * @param data
   * @param maxMarks
   * @param sampleSize
   * @return
   */
  def getMarks(data: RDD[IndexTuple[WorkingVector]], maxMarks: Int, sampleSize: Int): Marks = {
    val fraction = ADAMSamplingUtils.computeFractionForSampleSize(sampleSize, data.count(), false)

    getMarksForSample(data.sample(false, fraction), maxMarks)
  }

  /**
   * 
   * @param sample
   * @param maxMarks
   * @return
   */
  protected def getMarksForSample(sample: RDD[IndexTuple[WorkingVector]], maxMarks: Int): Marks
}
