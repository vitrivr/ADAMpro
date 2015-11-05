package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait FeatureStorage {
  def read(entityname: EntityName, filter: Option[scala.collection.Set[TupleID]] = None): DataFrame
  def write(tablename : EntityName, df: DataFrame, mode : SaveMode = SaveMode.Append): Unit
  def drop(tablename :EntityName) : Unit
}

