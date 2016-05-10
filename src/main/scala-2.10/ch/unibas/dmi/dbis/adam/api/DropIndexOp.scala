package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.index.IndexHandler
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.log4j.Logger

import scala.util.{Failure, Try}

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
  def apply(indexname: IndexName)(implicit ac: AdamContext): Try[Void] = {
    try {
      log.debug("perform drop index operation")
      IndexHandler.drop(indexname)
    } catch {
      case e : Exception => Failure(e)
    }
  }
}

