package ch.unibas.dmi.dbis.adam.catalog.catalogs

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.helpers.scanweight.ScanWeightBenchmarker
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
  private val DEFAULT_WEIGHT: Float = ScanWeightBenchmarker.DEFAULT_WEIGHT

  def indexname = column[String]("index")

  def weight = column[Float]("weight", O.Default(DEFAULT_WEIGHT))

  /**
    * Special fields
    */
  def pk = primaryKey("indexweight_pk", (indexname))

  def * = (indexname, weight)

  def index = foreignKey("indexweight_index_fk", indexname, TableQuery[IndexCatalog])(_.indexname, onUpdate=ForeignKeyAction.Cascade, onDelete=ForeignKeyAction.Cascade)
}
