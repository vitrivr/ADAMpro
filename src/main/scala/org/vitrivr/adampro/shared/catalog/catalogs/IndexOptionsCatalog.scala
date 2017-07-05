package org.vitrivr.adampro.shared.catalog.catalogs

import org.vitrivr.adampro.shared.catalog.CatalogManager
import slick.driver.DerbyDriver.api._

/**
  * ADAMpro
  *
  * Catalog to store metadata to each index.
  *
  * Ivan Giangreco
  * July 2016
  */
private[catalog] class IndexOptionsCatalog(tag: Tag) extends Table[(String, String, String)](tag, Some(CatalogManager.SCHEMA), "ap_indexoptions") {
  def indexname = column[String]("index")

  def key = column[String]("key")

  def value = column[String]("value")


  /**
    * Special fields
    */
  def pk = primaryKey("indexoptions_pk", (indexname, key))

  def * = (indexname, key, value)

  def index = foreignKey("indexoptions_index_fk", indexname, TableQuery[IndexCatalog])(_.indexname, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
}

