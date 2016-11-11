package org.vitrivr.adampro.catalog.catalogs

import org.vitrivr.adampro.catalog.CatalogOperator
import slick.driver.DerbyDriver.api._

/**
  * ADAMpro
  *
  * Catalog to store measurements.
  *
  * Ivan Giangreco
  * August 2016
  */
private[catalog] class QueryLog(tag: Tag) extends Table[(String, String, String, Array[Byte])](tag, Some(CatalogOperator.SCHEMA), "ap_querylog") {
  def key = column[String]("key", O.PrimaryKey)

  def entityname = column[String]("entity")

  def attribute = column[String]("attribute")

  def query = column[Array[Byte]]("query")

  /**
    * Special fields
    */
  def * = (key, entityname, attribute, query)
}