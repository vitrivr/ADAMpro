package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.data.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.data.types.Feature.WorkingVector
import ch.unibas.dmi.dbis.adam.index.Index.IndexTypeName


/**
 * 
 */
trait IndexScanner {
  val indexname: IndexTypeName

  /**
   * 
   */
  def query(q: WorkingVector, index: Index, options: Map[String, Any]): Seq[TupleID]
}
