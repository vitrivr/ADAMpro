package org.vitrivr.adampro.shared.catalog.catalogs

import org.vitrivr.adampro.shared.catalog.CatalogManager
import slick.driver.H2Driver.api._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
private[catalog] class StorageEngineOptionsCatalog(tag: Tag) extends Table[(String, String, String, String)](tag, Some(CatalogManager.SCHEMA), "ap_storenegineoptions") {
  def engine = column[String]("engine")

  def storename = column[String]("storename")

  def key = column[String]("key")

  def value = column[String]("value")


  /**
    * Special fields
    */
  def pk = primaryKey("storageengine_pk", (engine, storename, key))

  def * = (engine, storename, key, value)
}


