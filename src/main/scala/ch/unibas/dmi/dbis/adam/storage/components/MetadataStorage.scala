package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import org.apache.spark.Logging
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
    * @param entityname
    * @param fields
    */
  def create(entityname: EntityName, fields: Seq[AttributeDefinition]): Try[Option[String]] = Success(None)

  /**
    * Read data from metadata storage.
    *
    * @param tablename
    * @return
    */
  def read(tablename: EntityName): Try[DataFrame]

  /**
    * Write data to metadata storage.
    *
    * @param tablename
    * @param data
    * @param mode
    * @return
    */
  def write(tablename: EntityName, data: DataFrame, mode: SaveMode = SaveMode.Append): Try[String]

  /**
    * Drop data from the metadata storage.
    *
    * @param tablename
    * @return
    */
  def drop(tablename: EntityName): Try[Void]
}
