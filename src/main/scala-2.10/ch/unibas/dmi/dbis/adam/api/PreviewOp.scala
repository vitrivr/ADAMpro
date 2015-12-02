package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName

/**
 * adamtwo
 *
 * Ivan Giangreco
 * December 2015
 */
object PreviewOp {
  def apply(entityname: EntityName, k : Int = 100): Seq[String] =
    Entity.retrieveEntity(entityname).show(k).map(r => r.toString())
}
