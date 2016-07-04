package ch.unibas.dmi.dbis.adam.catalog.catalogs

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import slick.driver.PostgresDriver.api._

/**
  * ADAMpro
  *
  * Catalog to store all entities.
  *
  * Ivan Giangreco
  * June 2016
  */
private[catalog] class EntityCatalog(tag: Tag) extends Table[(String)](tag, Some(CatalogOperator.SCHEMA), "ap_entity") {
  def entityname = column[String]("entityname", O.PrimaryKey)

  /**
    * Special fields
    */
  def * = (entityname)

  def attributes = foreignKey("entity_attribute_fk", entityname, TableQuery[AttributeCatalog])(_.entityname)

  def indices = foreignKey("entity_indexes_fk", entityname, TableQuery[IndexCatalog])(_.entityname)

  def options = foreignKey("entityoptions_options_fk", entityname, TableQuery[EntityOptionsCatalog])(_.entityname)
}
