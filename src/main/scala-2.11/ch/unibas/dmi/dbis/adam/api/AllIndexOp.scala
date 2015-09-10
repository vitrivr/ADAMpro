package ch.unibas.dmi.dbis.adam.api

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
    //TODO: replace by enum
    IndexOp(tablename, "lsh", Map[String, String]())
    IndexOp(tablename, "slsh", Map[String, String]())
    IndexOp(tablename, "va", Map[String, String]())
  }
}
