package org.vitrivr.adampro.catalog.catalogs

import org.vitrivr.adampro.catalog.CatalogOperator
import slick.driver.DerbyDriver.api._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
private[catalog] class OptionsCatalog(tag: Tag) extends Table[(String, Array[Byte])](tag, Some(CatalogOperator.SCHEMA), "ap_options") {
  def key = column[String]("key", O.PrimaryKey)

  def value = column[Array[Byte]]("value")


  /**
    * Special fields
    */
  override def * = (key, value)
}