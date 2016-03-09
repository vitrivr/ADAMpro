package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName

/**
  * adamtwo
  *
  * Preview operation. Gives preview of entity.
  *
  * Ivan Giangreco
  * December 2015
  */
object PreviewOp {
  /**
    * Gives preview of entity.
    * @param entityname
    * @param k number of elements to show in preview
    * @return
    */
  def apply(entityname: EntityName, k: Int = 100): Seq[String] =
    Entity.load(entityname).show(k).map(r => r.toString())
}
