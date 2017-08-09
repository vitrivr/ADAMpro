package org.vitrivr.adampro.shared.catalog.catalogs

import org.vitrivr.adampro.shared.catalog.CatalogManager
import slick.driver.H2Driver.api._

/**
  * ADAMpro
  *
  * Catalog for storing metadata to each attribute.
  *
  * Ivan Giangreco
  * June 2016
  */
private[catalog] class AttributeOptionsCatalog(tag: Tag) extends Table[(String, String, String, String)](tag, Some(CatalogManager.SCHEMA), "ap_attributeoptions") {
  def entityname = column[String]("entity")

  def attributename = column[String]("attribute")

  def key = column[String]("key")

  def value = column[String]("value")


  /**
    * Special fields
    */
  def pk = primaryKey("attributeopt_pk", (entityname, attributename, key))

  def * = (entityname, attributename, key, value)

  def idx = index("idx_attributeoptions_entityname", entityname)
  def idx2 = index("idx_attributeoptions_attributename", attributename)
  def idx3 = index("idx_attributeoptions_key", key)

  def attribute = foreignKey("attributeoptions_attribute_fk", (entityname, attributename), TableQuery[AttributeCatalog])(t => (t.entityname, t.attributename), onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
}
