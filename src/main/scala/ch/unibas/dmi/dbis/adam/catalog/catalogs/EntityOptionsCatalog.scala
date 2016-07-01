package ch.unibas.dmi.dbis.adam.catalog.catalogs

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import slick.driver.PostgresDriver.api._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
private[catalog] class EntityOptionsCatalog(tag: Tag) extends Table[(String, String, String)](tag, Some(CatalogOperator.SCHEMA), "ap_entityoptions") {
  def entityname = column[String]("entityname")

  def key = column[String]("key")

  def value = column[String]("value")


  /**
    * Special fields
    */
  def pk = primaryKey("entityoptions_pk", (entityname, key))

  def * = (entityname, key, value)

  def entity = foreignKey("entityoptions_entity_fk", entityname, TableQuery[EntityCatalog])(_.entityname, onUpdate=ForeignKeyAction.Cascade, onDelete=ForeignKeyAction.Cascade)
}

