package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import org.apache.log4j.Logger

import scala.util.{Success, Failure, Try}

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
  def apply(): Try[Seq[EntityName]] = {
    try {
      log.debug("perform list entities operation")
      Success(EntityHandler.list)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}
