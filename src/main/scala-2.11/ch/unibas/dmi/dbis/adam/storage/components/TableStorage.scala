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
  def readTable(tablename: TableName): Table
  def writeTable(tablename : TableName, df: DataFrame, mode : SaveMode = SaveMode.Append): Unit
  def dropTable(tablename :TableName) : Unit
}

