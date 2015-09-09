package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table.TableName

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object CountOp {
  /**
   *
   * @param tablename
   */
  def apply(tablename : TableName): Long = {
    val table = Table.retrieveTable(tablename)
    table.count
  }
}