package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.sql.DataFrame

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
trait LazyTableStorage extends TableStorage {
  /**
   *
   * @param tablename
   * @param filter
   */
  def readFilteredTable(tablename : TableName, filter : Set[Long]) : DataFrame

  /**
   *
   * @param tablename
   */
  def readFullTable(tablename : TableName) : DataFrame
}
