package org.vitrivr.adampro.index.structures.va.marks

import org.vitrivr.adampro.index.IndexingTaskTuple
import org.vitrivr.adampro.index.structures.va.VAIndex.Marks


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
private[va] trait MarksGenerator extends Serializable {
  /**
    *
    * @param samples  training samples
    * @param maxMarks maximal number of marks (different for every dimension)
    * @return
    */
  private[va] def getMarks(samples: Seq[IndexingTaskTuple], maxMarks: Seq[Int]): Marks

  /**
    *
    * @param samples  training samples
    * @param maxMarks maximal number of marks (equal for every dimension)
    * @return
    */
  private[va] def getMarks(samples: Seq[IndexingTaskTuple], maxMarks: Int): Marks = {
    val dimensionality = samples.head.ap_indexable.length
    getMarks(samples, Seq.fill(dimensionality)(maxMarks))
  }
}
