package ch.unibas.dmi.dbis.adam.api

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
   * @param ifExists
   */
  def apply(tablename: TableName, ifExists : Boolean = false): Unit ={
    Table.dropTable(tablename, ifExists)
  }
}
