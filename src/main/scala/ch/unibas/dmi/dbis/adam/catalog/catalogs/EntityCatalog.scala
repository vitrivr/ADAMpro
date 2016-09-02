package ch.unibas.dmi.dbis.adam.catalog.catalogs

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import slick.driver.PostgresDriver.api._

/**
  * ADAMpro
  *
  * Catalog to store all entities.
  *
  * Ivan Giangreco
  * June 2016
  */
private[catalog] class EntityCatalog(tag: Tag) extends Table[(String)](tag, Some(CatalogOperator.SCHEMA), "ap_entity") {
  def entityname = column[String]("entity", O.PrimaryKey, O.Length(256))

  /**
    * Special fields
    */
  def * = (entityname)
}
