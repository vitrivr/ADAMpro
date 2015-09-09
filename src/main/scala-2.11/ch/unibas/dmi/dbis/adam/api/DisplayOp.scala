package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.Feature.VectorBase
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
  def apply(tablename: TableName) : Seq[(Long, Seq[VectorBase])] = {
    val results = Table.retrieveTable(tablename)
    results.show(100).map(row => (row.getLong(0), row.getSeq[VectorBase](1)))
  }
}
