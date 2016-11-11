package org.vitrivr.adampro.catalog.catalogs

import org.vitrivr.adampro.catalog.CatalogOperator
import slick.driver.DerbyDriver.api._

/**
  * ADAMpro
  *
  * Catalog to store measurements.
  *
  * Ivan Giangreco
  * August 2016
  */
private[catalog] class MeasurementLog(tag: Tag) extends Table[(String, String, Long, Long)](tag, Some(CatalogOperator.SCHEMA), "ap_measurementlog") {
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