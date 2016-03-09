package ch.unibas.dmi.dbis.adam.index.structures.va.marks

import ch.unibas.dmi.dbis.adam.index.IndexingTaskTuple
import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex.Marks


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
  private[va] def getMarks(sample: Array[IndexingTaskTuple], maxMarks: Seq[Int]): Marks

  /**
   *
   * @param sample
   * @param maxMarks
   * @return
   */
  private[va] def getMarks(sample: Array[IndexingTaskTuple], maxMarks: Int): Marks = {
    val dimensionality = sample.head.value.length
    getMarks(sample, Seq.fill(dimensionality)(maxMarks))
  }
}
