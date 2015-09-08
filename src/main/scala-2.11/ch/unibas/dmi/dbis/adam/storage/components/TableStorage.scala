package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait TableStorage {
  /**
   *
   * @param tablename
   * @return
   */
  def readTable(tablename: TableName): Table

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

