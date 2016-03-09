package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName

/**
  * adamtwo
  *
  * Drop operation. Drops an index.
  *
  *
  * Ivan Giangreco
  * March 2016
  */
object DropIndexOp {
  /**
    * Drops an index.
    *
    * @param indexname
    * @return true if index was dropped
    */
  def apply(indexname: IndexName): Boolean = Index.drop(indexname)
}

