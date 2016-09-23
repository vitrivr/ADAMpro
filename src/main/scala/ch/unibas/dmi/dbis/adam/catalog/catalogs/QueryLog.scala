package ch.unibas.dmi.dbis.adam.catalog.catalogs

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import slick.driver.DerbyDriver.api._

/**
  * ADAMpro
  *
  * Catalog to store measurements.
  *
  * Ivan Giangreco
  * August 2016
  */
private[catalog] class QueryLog(tag: Tag) extends Table[(String, String, Array[Byte])](tag, Some(CatalogOperator.SCHEMA), "ap_querylog") {
  def key = column[String]("key")

  def entityname = column[String]("entity")

  def query = column[Array[Byte]]("query")

  /**
    * Special fields
    */
  def * = (key, entityname, query)

  def idx = index("idx_querylog_key", (key))
}