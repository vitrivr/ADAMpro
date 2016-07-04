package ch.unibas.dmi.dbis.adam.catalog.catalogs

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import slick.driver.PostgresDriver.api._

/**
  * ADAMpro
  *
  * Catalog for storing metadata to each attribute.
  *
  * Ivan Giangreco
  * June 2016
  */
private[catalog] class AttributeOptionsCatalog(tag: Tag) extends Table[(String, String, String, String)](tag, Some(CatalogOperator.SCHEMA), "ap_attributeoptions") {
  def entityname = column[String]("entityname")

  def attributename = column[String]("attributename")

  def key = column[String]("key")

  def value = column[String]("value")


  /**
    * Special fields
    */
  def pk = primaryKey("attributeopt_pk", (entityname, attributename, key))

  def * = (entityname, attributename, key, value)

  def entity = foreignKey("attributeopt_entity_fk", entityname, TableQuery[EntityCatalog])(_.entityname)
}
