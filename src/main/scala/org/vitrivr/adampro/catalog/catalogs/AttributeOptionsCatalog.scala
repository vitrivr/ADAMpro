package org.vitrivr.adampro.catalog.catalogs

import org.vitrivr.adampro.catalog.CatalogOperator
import slick.driver.DerbyDriver.api._

/**
  * ADAMpro
  *
  * Catalog for storing metadata to each attribute.
  *
  * Ivan Giangreco
  * June 2016
  */
private[catalog] class AttributeOptionsCatalog(tag: Tag) extends Table[(String, String, String, String)](tag, Some(CatalogOperator.SCHEMA), "ap_attributeoptions") {
  def entityname = column[String]("entity")

  def attributename = column[String]("attribute")

  def key = column[String]("key")

  def value = column[String]("value")


  /**
    * Special fields
    */
  def pk = primaryKey("attributeopt_pk", (entityname, attributename, key))

  def * = (entityname, attributename, key, value)

  def attribute = foreignKey("attributeoptions_attribute_fk", (entityname, attributename), TableQuery[AttributeCatalog])(t => (t.entityname, t.attributename), onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
}
