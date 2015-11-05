package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import Feature.VectorBase
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._

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
  def apply(tablename: EntityName) : Seq[(Long, Seq[VectorBase])] = {
    val results = Entity.retrieveEntity(tablename)
    results.show(100).map(row => (row.getLong(0), row.getSeq[VectorBase](1)))
  }
}
