package org.vitrivr.adampro.exception

import org.vitrivr.adampro.entity.Entity.EntityName

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
case class EntityNotExistingException(message : String = "Entity not existing.")  extends GeneralAdamException(message)

object EntityNotExistingException {
  def withEntityname(entityname: EntityName): EntityNotExistingException = new EntityNotExistingException(s"Entity '$entityname' not existing.")
}


