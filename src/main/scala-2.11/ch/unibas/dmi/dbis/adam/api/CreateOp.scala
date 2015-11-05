package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
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
  def apply(tablename: EntityName, schema : StructType) : Unit = {
    Entity.createEntity(tablename, schema)
  }

}
