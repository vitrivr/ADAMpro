package org.vitrivr.adampro.data.index

import org.vitrivr.adampro.data.datatypes.TupleID.TupleID
import org.vitrivr.adampro.data.datatypes.vector.Vector.DenseMathVector

/**
  * adamtwo
  *
  * Tuple containing all data necessary at the indexing task (i.e., when the index is created).
  *
  * Ivan Giangreco
  * September 2015
  */
@SerialVersionUID(100L)
case class IndexingTaskTuple(ap_id: TupleID, ap_indexable: DenseMathVector) extends Serializable