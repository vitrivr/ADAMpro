package org.vitrivr.adampro.shared.catalog.catalogs

import org.vitrivr.adampro.shared.catalog.CatalogManager
import slick.driver.H2Driver.api._

/**
  * ADAMpro
  *
  * Catalog to store the indexes.
  *
  * Ivan Giangreco
  * June 2016
  */
private[catalog] class IndexCatalog(tag: Tag) extends Table[(String, String, String, String, Array[Byte], Boolean)](tag, Some(CatalogManager.SCHEMA), "ap_index") {
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