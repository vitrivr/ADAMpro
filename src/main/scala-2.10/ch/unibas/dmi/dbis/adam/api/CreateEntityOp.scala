package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._

/**
  * adamtwo
  *
  * Create operation. Creates an entity.
  *
  *
  * Ivan Giangreco
  * August 2015
  */
object CreateEntityOp {
  /**
    * Creates an entity.
    *
    * @param entityname
    * @param fields if fields is specified, in the metadata storage a table is created with these names, specify fields
    *               as key = name, value = SQL type
    * @return
    */
  def apply(entityname: EntityName, fields: Option[Map[String, String]] = None): Entity = {
    Entity.create(entityname)
  }
}
