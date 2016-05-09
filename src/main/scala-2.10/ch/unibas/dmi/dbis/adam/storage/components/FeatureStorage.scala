package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.FieldDefinition
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.util.Try

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
trait FeatureStorage extends Serializable {
  /**
    *
    * @param entityname
    * @return
    */
  def exists(entityname : EntityName) : Boolean

  /**
    * Create the entity in the feature storage.
    *
    * @param entityname
    * @return true on success
    */
  def create(entityname: EntityName, fields : Seq[FieldDefinition])(implicit ac: AdamContext): Boolean = {
    val structFields = fields.map {
      field => StructField(field.name, field.fieldtype.datatype)
    }

    val df = sqlContext.createDataFrame(sc.emptyRDD[Row], StructType(structFields))
    write(entityname, fields.filter(_.pk).head.name, df, SaveMode.Overwrite)
  }

  /**
    * Read entity from feature storage.
    *
    * @param entityname
    * @return
    */
  def read(entityname: EntityName)(implicit ac : AdamContext): Try[DataFrame]

  /**
    * Count the number of tuples in the feature storage.
    *
    * @param entityname
    * @return
    */
  def count(entityname: EntityName)(implicit ac: AdamContext): Long

  /**
    * Write entity to the feature storage.
    *
    * @param entityname
    * @param df
    * @param mode
    * @return true on success
    */
  def write(entityname: EntityName, pk : String, df: DataFrame, mode: SaveMode = SaveMode.Append)(implicit ac: AdamContext): Boolean

  /**
    * Drop the entity from the feature storage
    *
    * @param entityname
    * @return true on success
    */
  def drop(entityname: EntityName)(implicit ac: AdamContext): Boolean
}

