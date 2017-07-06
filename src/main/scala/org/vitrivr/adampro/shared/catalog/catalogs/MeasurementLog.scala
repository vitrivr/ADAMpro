package org.vitrivr.adampro.shared.catalog.catalogs

import org.vitrivr.adampro.shared.catalog.CatalogManager
import slick.driver.H2Driver.api._

/**
  * ADAMpro
  *
  * Catalog to store measurements.
  *
  * Ivan Giangreco
  * August 2016
  */
private[catalog] class MeasurementLog(tag: Tag) extends Table[(String, String, Long, Long)](tag, Some(CatalogManager.SCHEMA), "ap_measurementlog") {
  def key = column[String]("key")

  def source = column[String]("source")

  def nresults = column[Long]("nresults")

  def time = column[Long]("time")


  /**
    * Special fields
    */
  def * = (key, source, nresults, time)

  def entity = foreignKey("measurementlog_querylog_fk", key, TableQuery[QueryLog])(_.key, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
}