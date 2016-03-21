package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import org.apache.log4j.Logger

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
  def apply(entityname: EntityName, ifExists: Boolean = false): Boolean = {
    log.debug("perform drop entity operation")
    Entity.drop(entityname, ifExists)
  }
}
