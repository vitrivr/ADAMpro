package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object ListOp {
  def apply() : Seq[String] = CatalogOperator.listEntities()
}
