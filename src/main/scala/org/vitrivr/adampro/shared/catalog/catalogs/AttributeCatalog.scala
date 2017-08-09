package org.vitrivr.adampro.shared.catalog.catalogs

import org.vitrivr.adampro.shared.catalog.CatalogManager
import slick.driver.H2Driver.api._

/**
  * ADAMpro
  *
  * Catalog for storing the attributes to each entity.
  *
  * Ivan Giangreco
  * June 2016
  */
private[catalog] class AttributeCatalog(tag: Tag) extends Table[(String, String, String, Boolean, String)](tag, Some(CatalogManager.SCHEMA), "ap_attribute") {
  //TODO: possibly store order of attribute

  def entityname = column[String]("entity")

  def attributename = column[String]("attribute")

  def attributetype = column[String]("attributetype")

  def isPK = column[Boolean]("ispk")

  def handlername = column[String]("handler")

  /**
    * Special fields
    */
  def pk = primaryKey("attribute_pk", (entityname, attributename))

  def * = (entityname, attributename, attributetype, isPK, handlername)

  def idx = index("idx_attribute_entityname", entityname)
  def idx2 = index("idx_attribute_attributename", attributename)
  def idx3 = index("idx_attribute_pk", isPK)

  def entity = foreignKey("attribute_entity_fk", entityname, TableQuery[EntityCatalog])(_.entityname, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
}
