package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.entity.Entity._

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
  def apply(tablename : EntityName, distance : DistanceFunction): Unit = {
    IndexStructures.values.foreach({ structure =>
      IndexOp(tablename, structure, distance, Map[String, String]())
    })
  }
}
