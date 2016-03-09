package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._

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
  /**
    * Returns number of elements in entity (only feature storage is considered).
    * @param entityname
    * @return the number of tuples in the entity
    */
  def apply(entityname: EntityName): Long = Entity.countTuples(entityname)
}