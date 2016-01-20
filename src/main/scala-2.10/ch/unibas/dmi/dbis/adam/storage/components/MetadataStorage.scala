package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.entity.Entity._
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
trait MetadataStorage {
  def create(entityname : EntityName, df: DataFrame) = write(entityname, df, SaveMode.Overwrite)
  def read(tablename: EntityName): DataFrame
  def write(tablename : EntityName, df: DataFrame, mode : SaveMode = SaveMode.Append): Unit
  def drop(tablename :EntityName) : Unit
}
