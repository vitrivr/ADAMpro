package ch.unibas.dmi.dbis.adam.cli.operations

import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table.TableName

import org.apache.spark.sql.types._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object CreateOp {
  /**
   *
   * @param tablename
   * @param schema
   */
  def apply(tablename: TableName, schema : StructType) : Unit = {
    Table.createTable(tablename, schema)
  }

}
