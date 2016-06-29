package ch.unibas.dmi.dbis.adam.handler

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.engine.ParquetFeatureStorage
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object FeatureDatabaseHandler extends Handler with Serializable {
  override def supportedFields = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.STRINGTYPE, FieldTypes.FEATURETYPE)

  override val name: String = "feature"

  override def create(entityname: EntityName, path : Option[String], fields: Seq[AttributeDefinition], params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Option[String]] = ParquetFeatureStorage.create(entityname, fields)

  override def count(entityname: EntityName, path : Option[String], params: Map[String, String])(implicit ac: AdamContext): Try[Long] = ParquetFeatureStorage.read(path.get).map(_.count())

  override def drop(entityname: EntityName, path : Option[String], params: Map[String, String])(implicit ac: AdamContext): Try[Void] = ParquetFeatureStorage.drop(path.get)

  override def write(entityname: EntityName, path : Option[String], df: DataFrame, mode: SaveMode, params: Map[String, String])(implicit ac: AdamContext): Try[String] = ParquetFeatureStorage.write(entityname, df, mode, path)

  override def read(entityname: EntityName, path : Option[String], params: Map[String, String])(implicit ac: AdamContext): Try[DataFrame] = ParquetFeatureStorage.read(path.get)

  override def exists(entityname: EntityName, path : Option[String], params: Map[String, String])(implicit ac: AdamContext): Try[Boolean] = ParquetFeatureStorage.read(path.get).map(x => true)
}
