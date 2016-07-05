package ch.unibas.dmi.dbis.adam.catalog.catalogs

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import slick.driver.PostgresDriver.api._

/**
  * ADAMpro
  *
  * Catalog to store the weights to each index.
  *
  * Ivan Giangreco
  * June 2016
  */
private[catalog] class IndexWeightCatalog(tag: Tag) extends Table[(String, Float)](tag, Some(CatalogOperator.SCHEMA), "ap_indexweight") {
  def indexname = column[String]("index")

  def weight = column[Float]("weight")

  /**
    * Special fields
    */
  def pk = primaryKey("indexweight_pk", (indexname))

  def * = (indexname, weight)

  def index = foreignKey("indexweight_index_fk", indexname, TableQuery[IndexCatalog])(_.indexname, onUpdate=ForeignKeyAction.Cascade, onDelete=ForeignKeyAction.Cascade)
}
