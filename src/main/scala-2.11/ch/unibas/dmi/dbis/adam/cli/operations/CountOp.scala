package ch.unibas.dmi.dbis.adam.cli.operations

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
  def apply(tablename : TableName): Unit = {
    val table = Table.retrieveTable(tablename)
    println(table.count)
  }
}