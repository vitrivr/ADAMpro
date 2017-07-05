package org.vitrivr.adampro.shared.catalog.catalogs

import org.vitrivr.adampro.shared.catalog.CatalogManager
import slick.driver.DerbyDriver.api._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * November 2016
  */
private[catalog] class OptionsCatalog(tag: Tag) extends Table[(String, Array[Byte])](tag, Some(CatalogManager.SCHEMA), "ap_options") {
  def key = column[String]("key", O.PrimaryKey)

  def value = column[Array[Byte]]("value")


  /**
    * Special fields
    */
  override def * = (key, value)
}