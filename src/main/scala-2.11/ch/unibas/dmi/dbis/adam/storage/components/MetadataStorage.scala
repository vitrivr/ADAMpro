package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
trait MetadataStorage {
  /**
   *
   * @param tablename
   * @return
   */
  def readTable(tablename: TableName): DataFrame

  /**
   *
   * @param tablename
   * @param df
   * @param mode
   */
  def writeTable(tablename : TableName, df: DataFrame, mode : SaveMode = SaveMode.Append): Unit

  /**
   *
   * @param tablename
   */
  def dropTable(tablename :TableName) : Unit
}
