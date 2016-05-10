package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import ch.unibas.dmi.dbis.adam.main.AdamContext

import scala.util.{Failure, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * May 2016
  */
object PropertiesOp {
  def apply(entityname: EntityName)(implicit ac: AdamContext): Try[Map[String, String]] = {
    try {
      EntityHandler.getProperties(entityname)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}
