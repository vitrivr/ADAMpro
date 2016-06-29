package ch.unibas.dmi.dbis.adam.handler

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
trait Handler extends Serializable {
  val name : String
  def supportedFields : Seq[FieldType]

  def create(entityname: EntityName, path : Option[String], fields: Seq[AttributeDefinition], params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Option[String]]
  def exists(entityName: EntityName, path : Option[String], params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Boolean]
  def read(tablename: EntityName, path : Option[String], params : Map[String, String] = Map())(implicit ac: AdamContext): Try[DataFrame]
  def count(entityName: EntityName, path : Option[String], params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Long]
  def write(entityname: EntityName, path : Option[String], df: DataFrame, mode: SaveMode = SaveMode.Append, params : Map[String, String] = Map())(implicit ac: AdamContext): Try[String]
  def drop(entityName: EntityName, path : Option[String], params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Void]
}
