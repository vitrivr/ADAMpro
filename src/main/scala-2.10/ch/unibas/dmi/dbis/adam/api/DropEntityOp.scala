package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.log4j.Logger

import scala.util.Try

/**
  * adamtwo
  *
  * Drop operation. Drops an entity.
  *
  * Ivan Giangreco
  * September 2015
  */
object DropEntityOp {
  val log = Logger.getLogger(getClass.getName)

  /**
    * Drops an entity.
    *
    * @param entityname
    * @param ifExists
    * @return
    */
  def apply(entityname: EntityName, ifExists: Boolean = false)(implicit ac: AdamContext): Try[Void] = {
    log.debug("perform drop entity operation")
    EntityHandler.drop(entityname, ifExists)
  }
}
