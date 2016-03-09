package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity

/**
  * adamtwo
  *
  * List operation. Lists entities.
  *
  * Ivan Giangreco
  * August 2015
  */
object ListEntityOp {
  /**
    * Lists names of all entities.
 *
    * @return
    */
  def apply(): Seq[String] = Entity.list
}
