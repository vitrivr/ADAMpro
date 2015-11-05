package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object DropOp {
  /**
   *
   * @param tablename
   * @param ifExists
   */
  def apply(tablename: EntityName, ifExists : Boolean = false): Unit ={
    Entity.dropEntity(tablename, ifExists)
  }
}
