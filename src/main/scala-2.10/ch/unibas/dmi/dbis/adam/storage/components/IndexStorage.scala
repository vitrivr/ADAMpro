package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import org.apache.spark.sql.DataFrame

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait IndexStorage {
  def create(entityname : EntityName, df: DataFrame) = write(entityname, df)
  def read(indexName : IndexName) : DataFrame
  def write(indexName: IndexName, index: DataFrame): Unit
  def drop(indexName : IndexName) : Unit
}
