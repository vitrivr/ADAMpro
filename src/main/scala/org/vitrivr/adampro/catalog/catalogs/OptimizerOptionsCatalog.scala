package org.vitrivr.adampro.catalog.catalogs

import org.vitrivr.adampro.catalog.CatalogOperator
import slick.driver.DerbyDriver.api._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
private[catalog] class OptimizerOptionsCatalog(tag: Tag) extends Table[(String, String, Array[Byte])](tag, Some(CatalogOperator.SCHEMA), "ap_optimizeroptions") {
  def optimizer = column[String]("optimizer")

  def key = column[String]("key")

  def value = column[Array[Byte]]("value")


  /**
    * Special fields
    */
  def pk = primaryKey("optimizeroptions_pk", (optimizer, key))

  override def * = (optimizer, key, value)
}