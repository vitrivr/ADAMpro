package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import org.apache.log4j.Logger

/**
  * adamtwo
  *
  * List operation. Lists entities.
  *
  * Ivan Giangreco
  * August 2015
  */
object ListEntitiesOp {
  val log = Logger.getLogger(getClass.getName)

  /**
    * Lists names of all entities.
 *
    * @return
    */
  def apply(): Seq[EntityName] = {
    log.debug("perform list entities operation")
    EntityHandler.list
  }
}
