package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import org.apache.log4j.Logger

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
  val log = Logger.getLogger(getClass.getName)

  /**
    * Drops an index.
    *
    * @param indexname
    * @return true if index was dropped
    */
  def apply(indexname: IndexName): Boolean = {
    log.debug("perform drop index operation")
    Index.drop(indexname).isSuccess
  }
}

