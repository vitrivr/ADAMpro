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
  def entityname = column[String]("entity")

  def attributename = column[String]("attribute")

  def fieldtype = column[String]("fieldtype")

  def isPK = column[Boolean]("ispk")

  def isUnique = column[Boolean]("isunique")

  def isIndexed = column[Boolean]("isindexed")

  def handlername = column[String]("handler")

  /**
    * Special fields
    */
  def pk = primaryKey("attribute_pk", (entityname, attributename))

  def * = (entityname, attributename, fieldtype, isPK, isUnique, isIndexed, handlername)

  def idx = index("idx_attribute_entityname", entityname)

  def entity = foreignKey("attribute_entity_fk", entityname, TableQuery[EntityCatalog])(_.entityname, onUpdate=ForeignKeyAction.Cascade, onDelete=ForeignKeyAction.Cascade)
}
