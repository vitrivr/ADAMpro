package ch.unibas.dmi.dbis.adam.catalog.catalogs

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import slick.driver.PostgresDriver.api._

/**
  * ADAMpro
  *
  * Catalog to store metadata to each index.
  *
  * Ivan Giangreco
  * July 2016
  */
private[catalog] class IndexOptionsCatalog(tag: Tag) extends Table[(String, String, String)](tag, Some(CatalogOperator.SCHEMA), "ap_indexoptions") {
  def indexname = column[String]("indexname")

  def key = column[String]("key")

  def value = column[String]("value")


  /**
    * Special fields
    */
  def pk = primaryKey("indexoptions_pk", (indexname, key))

  def * = (indexname, key, value)

  def index = foreignKey("indexoptions_index_fk", indexname, TableQuery[IndexCatalog])(_.indexname, onUpdate=ForeignKeyAction.Cascade, onDelete=ForeignKeyAction.Cascade)
}

