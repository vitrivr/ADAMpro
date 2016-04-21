package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.FieldDefinition
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
trait MetadataStorage {
  /**
    * Create entity in metadata storage.
    *
    * @param entityname
    * @param fields
    */
  def create(entityname : EntityName, fields : Seq[FieldDefinition]) : Boolean

  /**
    * Read data from metadata storage.
    *
    * @param tablename
    * @return
    */
  def read(tablename: EntityName): DataFrame

  /**
    * Write data to metadata storage.
    *
    * @param tablename
    * @param data
    * @param mode
    * @return
    */
  def write(tablename : EntityName, data: DataFrame, mode : SaveMode = SaveMode.Append): Boolean

  /**
    * Drop data from the metadata storage.
    *
    * @param tablename
    * @return
    */
  def drop(tablename :EntityName) : Boolean
}
