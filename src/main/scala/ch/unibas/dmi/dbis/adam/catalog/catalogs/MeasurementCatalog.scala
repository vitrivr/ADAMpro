package ch.unibas.dmi.dbis.adam.catalog.catalogs

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import slick.driver.PostgresDriver.api._

/**
  * ADAMpro
  *
  * Catalog to store measurements.
  *
  * Ivan Giangreco
  * August 2016
  */
private[catalog] class MeasurementCatalog(tag: Tag) extends Table[(String, Long)](tag, Some(CatalogOperator.SCHEMA), "ap_measurement") {
  def key = column[String]("key")

  def measurement = column[Long]("measurement")

  /**
    * Special fields
    */
  def * = (key, measurement)

  def idx = index("idx_measurement_key", (key))
}