package org.vitrivr.adampro.shared.catalog.catalogs

import org.vitrivr.adampro.shared.catalog.CatalogManager
import slick.driver.DerbyDriver.api._

/**
  * ADAMpro
  *
  * Catalog to store measurements.
  *
  * Ivan Giangreco
  * August 2016
  */
private[catalog] class QueryLog(tag: Tag) extends Table[(String, String, String, Array[Byte])](tag, Some(CatalogManager.SCHEMA), "ap_querylog") {
  def key = column[String]("key", O.PrimaryKey)

  def entityname = column[String]("entity")

  def attribute = column[String]("attribute")

  def query = column[Array[Byte]]("query")

  /**
    * Special fields
    */
  def * = (key, entityname, attribute, query)
}