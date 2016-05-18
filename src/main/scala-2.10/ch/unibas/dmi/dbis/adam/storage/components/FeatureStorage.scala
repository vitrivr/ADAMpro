package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.FieldDefinition
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits._
import org.apache.spark.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.util.{Failure, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
trait FeatureStorage extends Serializable with Logging {
  /**
    *
    * @param path
    * @return
    */
  def exists(path: String): Try[Boolean]

  /**
    * Create the entity in the feature storage.
    *
    * @param entityname
    * @return true on success
    */
  def create(entityname: EntityName, fields: Seq[FieldDefinition])(implicit ac: AdamContext): Try[String] = {
    try {
      val structFields = fields.map {
        field => StructField(field.name, field.fieldtype.datatype)
      }

      val df = sqlContext.createDataFrame(sc.emptyRDD[Row], StructType(structFields))
      write(entityname, df, SaveMode.Overwrite)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Read entity from feature storage.
    *
    * @param path
    * @return
    */
  def read(path : String)(implicit ac: AdamContext): Try[DataFrame]

  /**
    * Count the number of tuples in the feature storage.
    *
    * @param path
    * @return
    */
  def count(path : String)(implicit ac: AdamContext): Try[Long]

  /**
    * Write entity to the feature storage.
    *
    * @param path
    * @param df
    * @param mode
    * @return true on success
    */
  def write(entityname : EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append, path : Option[String] = None)(implicit ac: AdamContext): Try[String]

  /**
    * Drop the entity from the feature storage
    *
    * @param path
    * @return true on success
    */
  def drop(path : String)(implicit ac: AdamContext): Try[Void]
}

