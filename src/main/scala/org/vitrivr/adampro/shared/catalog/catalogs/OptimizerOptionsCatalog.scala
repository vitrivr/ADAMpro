package org.vitrivr.adampro.shared.catalog.catalogs

import org.vitrivr.adampro.shared.catalog.CatalogManager
import slick.driver.DerbyDriver.api._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
private[catalog] class OptimizerOptionsCatalog(tag: Tag) extends Table[(String, String, Array[Byte])](tag, Some(CatalogManager.SCHEMA), "ap_optimizeroptions") {
  def optimizer = column[String]("optimizer")

  def key = column[String]("key")

  def value = column[Array[Byte]]("value")


  /**
    * Special fields
    */
  def pk = primaryKey("optimizeroptions_pk", (optimizer, key))

  override def * = (optimizer, key, value)
}