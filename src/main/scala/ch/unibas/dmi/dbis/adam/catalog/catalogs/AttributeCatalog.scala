package ch.unibas.dmi.dbis.adam.catalog.catalogs

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import slick.driver.PostgresDriver.api._

/**
  * ADAMpro
  *
  * Catalog for storing the attributes to each entity.
  *
  * Ivan Giangreco
  * June 2016
  */
private[catalog] class AttributeCatalog(tag: Tag) extends Table[(String, String, String, Boolean, Boolean, Boolean, String)](tag, Some(CatalogOperator.SCHEMA), "ap_attribute") {
  def entityname = column[String]("entityname")

  def attributename = column[String]("attribute")

  def fieldtype = column[String]("fieldtype")

  def isPK = column[Boolean]("ispk")

  def isUnique = column[Boolean]("isunique")

  def isIndexed = column[Boolean]("isindexed")

  def handler = column[String]("handler")

  /**
    * Special fields
    */
  def pk = primaryKey("attribute_pk", (entityname, attributename))

  def * = (entityname, attributename, fieldtype, isPK, isUnique, isIndexed, handler)

  def entity = foreignKey("attribute_entity_fk", entityname, TableQuery[EntityCatalog])(_.entityname, onUpdate=ForeignKeyAction.Cascade, onDelete=ForeignKeyAction.Cascade)

  def options = foreignKey("attribute_attributeOptions_fk", (entityname, attributename), TableQuery[AttributeOptionsCatalog])(t => (t.entityname, t.attributename))

  def weights = foreignKey("attribute_weights_fk", (entityname, attributename), TableQuery[AttributeOptionsCatalog])(t => (t.entityname, t.attributename))
}
