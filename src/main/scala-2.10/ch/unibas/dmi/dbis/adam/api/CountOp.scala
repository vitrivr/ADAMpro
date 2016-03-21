package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import org.apache.log4j.Logger

/**
  * adamtwo
  *
  * Count operation. Returns number of elements in entity (only feature storage is considered).
  *
  *
  * Ivan Giangreco
  * August 2015
  */
object CountOp {
  val log = Logger.getLogger(getClass.getName)

  /**
    * Returns number of elements in entity (only feature storage is considered).
    *
    * @param entityname
    * @return the number of tuples in the entity
    */
  def apply(entityname: EntityName): Long = {
    log.debug("perform count operation")
    Entity.countTuples(entityname)
  }
}