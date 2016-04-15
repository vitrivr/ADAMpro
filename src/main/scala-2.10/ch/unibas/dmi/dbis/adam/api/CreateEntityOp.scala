package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.{EntityHandler, Entity, FieldDefinition}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.log4j.Logger

import scala.util.Try

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
  val log = Logger.getLogger(getClass.getName)

  /**
    * Creates an entity.
    *
    * @param entityname
    * @param fields if fields is specified, in the metadata storage a table is created with these names, specify fields
    *               as key = name, value = field definition
    * @return
    */
  def apply(entityname: EntityName, fields: Option[Map[String, FieldDefinition]] = None)(implicit ac: AdamContext): Try[Entity] = {
    log.debug("perform create entity operation")
    EntityHandler.create(entityname, fields)
  }
}
