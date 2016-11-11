package org.vitrivr.adampro.catalog.catalogs

import org.vitrivr.adampro.catalog.CatalogOperator
import slick.driver.DerbyDriver.api._

/**
  * ADAMpro
  *
  * Catalog to store the indexes.
  *
  * Ivan Giangreco
  * June 2016
  */
private[catalog] class IndexCatalog(tag: Tag) extends Table[(String, String, String, String, Array[Byte], Boolean)](tag, Some(CatalogOperator.SCHEMA), "ap_index") {
  def indexname = column[String]("index", O.PrimaryKey)

  def entityname = column[String]("entity")

  def attributename = column[String]("attribute")

  def indextypename = column[String]("indextype")

  def meta = column[Array[Byte]]("meta")

  def isUpToDate = column[Boolean]("uptodate")

  /**
    * Special fields
    */
  def * = (indexname, entityname, attributename, indextypename, meta, isUpToDate)

  def idx = index("idx_index_entityname", entityname)

  def attribute = foreignKey("index_attribute_fk", (entityname, attributename), TableQuery[AttributeCatalog])(t => (t.entityname, t.attributename), onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
}