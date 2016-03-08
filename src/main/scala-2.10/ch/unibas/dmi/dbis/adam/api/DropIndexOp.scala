package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class DropIndexOp {
  def apply(indexname: IndexName): Boolean = Index.dropIndex(indexname)
}

