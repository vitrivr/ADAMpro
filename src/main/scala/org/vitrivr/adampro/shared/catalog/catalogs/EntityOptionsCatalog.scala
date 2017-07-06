package org.vitrivr.adampro.shared.catalog.catalogs

import org.vitrivr.adampro.shared.catalog.CatalogManager
import slick.driver.H2Driver.api._

/**
  * ADAMpro
  *
  * Catalog to store metadata to each entity.
  *
  * Ivan Giangreco
  * July 2016
  */
private[catalog] class EntityOptionsCatalog(tag: Tag) extends Table[(String, String, String)](tag, Some(CatalogManager.SCHEMA), "ap_entityoptions") {
  def entityname = column[String]("entity")

  def key = column[String]("key")

  def value = column[String]("value")


  /**
    * Special fields
    */
  def pk = primaryKey("entityoptions_pk", (entityname, key))

  def * = (entityname, key, value)

  def entity = foreignKey("entityoptions_entity_fk", entityname, TableQuery[EntityCatalog])(_.entityname, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
}

