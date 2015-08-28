package ch.unibas.dmi.dbis.adam.cli.operations

import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object DisplayOp {
  /**
   *
   * @param tablename
   */
  def apply(tablename: TableName) : Unit = {
    val results = Table.retrieveTable(tablename)

    if(results.count > 100){
      results.show(100)
      println("displaying 100 out of " + results.count)
    } else {
      results.show
    }
  }
}
