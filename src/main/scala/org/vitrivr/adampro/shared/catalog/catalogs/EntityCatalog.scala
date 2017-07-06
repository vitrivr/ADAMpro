package org.vitrivr.adampro.shared.catalog.catalogs

import org.vitrivr.adampro.shared.catalog.CatalogManager
import slick.driver.H2Driver.api._

/**
  * ADAMpro
  *
  * Catalog to store all entities.
  *
  * Ivan Giangreco
  * June 2016
  */
private[catalog] class EntityCatalog(tag: Tag) extends Table[(String)](tag, Some(CatalogManager.SCHEMA), "ap_entity") {
  def entityname = column[String]("entity", O.PrimaryKey)

  /**
    * Special fields
    */
  def * = (entityname)
}
