package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
trait MetadataStorage extends Serializable with Logging {
  /**
    * Create entity in metadata storage.
    *
    * @param tablename name of table
    * @param attributes attributes
    */
  def create(tablename: EntityName, attributes: Seq[AttributeDefinition])(implicit ac: AdamContext): Try[Option[String]] = Success(None)

  /**
    * Read data from metadata storage.
    *
    * @param tablename name of table
    * @return
    */
  def read(tablename: EntityName)(implicit ac: AdamContext): Try[DataFrame]

  /**
    * Write data to metadata storage.
    *
    * @param tablename name of table
    * @param data data
    * @param mode save mode (append, overwrite, ...)
    * @return
    */
  def write(tablename: EntityName, data: DataFrame, mode: SaveMode = SaveMode.Append)(implicit ac: AdamContext): Try[String]

  /**
    * Drop data from the metadata storage.
    *
    * @param tablename name of table
    * @return
    */
  def drop(tablename: EntityName)(implicit ac: AdamContext): Try[Void]
}
