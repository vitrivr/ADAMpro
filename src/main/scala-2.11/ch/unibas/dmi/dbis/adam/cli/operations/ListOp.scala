package ch.unibas.dmi.dbis.adam.cli.operations

import ch.unibas.dmi.dbis.adam.storage.catalog.CatalogOperator

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object ListOp {
  def apply() : Unit = {
    val tables = CatalogOperator.listTables()
    println(tables.mkString(", \n"))
  }
}
