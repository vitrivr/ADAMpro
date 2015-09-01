package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import org.apache.spark.sql.DataFrame

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait IndexStorage {
  def readIndex(indexName : IndexName) : DataFrame
  def writeIndex(indexName: IndexName, index: DataFrame): Unit
  def dropIndex(indexName : IndexName) : Unit
}
