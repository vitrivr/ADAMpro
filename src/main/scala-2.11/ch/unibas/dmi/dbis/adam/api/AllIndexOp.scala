package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.table.Table._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object AllIndexOp {
  /**
   *
   * @param tablename
   */
  def apply(tablename : TableName): Unit = {
    IndexStructures.values.foreach({ structure =>
      IndexOp(tablename, structure, Map[String, String]())
    })
  }
}
