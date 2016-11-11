package org.vitrivr.adampro.catalog.catalogs

import org.vitrivr.adampro.catalog.CatalogOperator
import slick.driver.DerbyDriver.api._

/**
  * ADAMpro
  *
  * Catalog for storing the attributes to each entity.
  *
  * Ivan Giangreco
  * June 2016
  */
private[catalog] class AttributeCatalog(tag: Tag) extends Table[(String, String, String, Boolean, String)](tag, Some(CatalogOperator.SCHEMA), "ap_attribute") {
  //TODO: possibly store order of attribute

  def entityname = column[String]("entity")

  def attributename = column[String]("attribute")

  def fieldtype = column[String]("fieldtype")

  def isPK = column[Boolean]("ispk")

  def handlername = column[String]("handler")

  /**
    * Special fields
    */
  def pk = primaryKey("attribute_pk", (entityname, attributename))

  def * = (entityname, attributename, fieldtype, isPK, handlername)

  def idx = index("idx_attribute_entityname", entityname)

  def entity = foreignKey("attribute_entity_fk", entityname, TableQuery[EntityCatalog])(_.entityname, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
}
