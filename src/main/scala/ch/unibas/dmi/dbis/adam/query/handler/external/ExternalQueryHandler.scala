package ch.unibas.dmi.dbis.adam.query.handler.external

import ch.unibas.dmi.dbis.adam.entity.Entity._
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
trait ExternalQueryHandler {
  def key : String

  def query(entityname: EntityName, query: Map[String, String], joinfield: Option[String]): DataFrame
  def insert(data: DataFrame, options : Map[String, String]) : Try[Void]
}
