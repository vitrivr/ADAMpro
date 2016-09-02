package ch.unibas.dmi.dbis.adam.catalog.catalogs

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import slick.driver.PostgresDriver.api._

/**
  * ADAMpro
  *
  * Catalog to store metadata to each entity.
  *
  * Ivan Giangreco
  * July 2016
  */
private[catalog] class EntityOptionsCatalog(tag: Tag) extends Table[(String, String, String)](tag, Some(CatalogOperator.SCHEMA), "ap_entityoptions") {
  def entityname = column[String]("entity", O.Length(256))

  def key = column[String]("key", O.Length(256))

  def value = column[String]("value", O.Length(256))


  /**
    * Special fields
    */
  def pk = primaryKey("entityoptions_pk", (entityname, key))

  def * = (entityname, key, value)

  def entity = foreignKey("entityoptions_entity_fk", entityname, TableQuery[EntityCatalog])(_.entityname, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
}

