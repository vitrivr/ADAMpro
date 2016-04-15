package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.log4j.Logger

/**
  * adamtwo
  *
  * Preview operation. Gives preview of entity.
  *
  * Ivan Giangreco
  * December 2015
  */
object PreviewOp {
  val log = Logger.getLogger(getClass.getName)

  /**
    * Gives preview of entity.
    *
    * @param entityname
    * @param k number of elements to show in preview
    * @return
    */
  def apply(entityname: EntityName, k: Int = 100)(implicit ac: AdamContext): Seq[String] = {
    log.debug("perform preview entity operation")
    EntityHandler.load(entityname).get.show(k).map(r => r.toString())
  }
}
