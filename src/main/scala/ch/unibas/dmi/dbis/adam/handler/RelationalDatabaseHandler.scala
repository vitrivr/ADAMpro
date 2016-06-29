package ch.unibas.dmi.dbis.adam.handler

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.engine.PostgresqlMetadataStorage
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object RelationalDatabaseHandler extends Handler with Serializable {
  override def supportedFields = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.FLOATTYPE, FieldTypes.DOUBLETYPE, FieldTypes.STRINGTYPE, FieldTypes.BOOLEANTYPE)

  override val name: String = "relational"

  override def create(entityname: EntityName, path : Option[String], fields: Seq[AttributeDefinition], params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Option[String]] = PostgresqlMetadataStorage.create(entityname, fields)

  override def count(entityname: EntityName, path : Option[String], params: Map[String, String])(implicit ac: AdamContext): Try[Long] = PostgresqlMetadataStorage.read(entityname).map(_.count())

  override def drop(entityname: EntityName, path : Option[String], params: Map[String, String])(implicit ac: AdamContext): Try[Void] = PostgresqlMetadataStorage.drop(entityname)

  override def write(entityname: EntityName, path : Option[String], df: DataFrame, mode: SaveMode, params: Map[String, String])(implicit ac: AdamContext): Try[String] = PostgresqlMetadataStorage.write(entityname, df, mode)

  override def read(entityname: EntityName, path : Option[String], params: Map[String, String])(implicit ac: AdamContext): Try[DataFrame] = PostgresqlMetadataStorage.read(entityname)

  override def exists(entityname: EntityName, path : Option[String], params: Map[String, String])(implicit ac: AdamContext): Try[Boolean] = PostgresqlMetadataStorage.read(entityname).map(x => true)
}