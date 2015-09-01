package ch.unibas.dmi.dbis.adam.cli.operations

import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object DropOp {
  /**
   *
   * @param tablename
   */
  def apply(tablename: TableName): Unit ={
    Table.dropTable(tablename)
  }
}
