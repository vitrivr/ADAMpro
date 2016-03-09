package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._

/**
  * adamtwo
  *
  * Drop operation. Drops an entity.
  *
  * Ivan Giangreco
  * September 2015
  */
object DropEntityOp {
  /**
    * Drops an entity.
    *
    * @param entityname
    * @param ifExists
    * @return
    */
  def apply(entityname: EntityName, ifExists: Boolean = false): Boolean = Entity.drop(entityname, ifExists)
}
