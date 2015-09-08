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
  /**
   *
   * @param indexName
   * @return
   */
  def readIndex(indexName : IndexName) : DataFrame

  /**
   *
   * @param indexName
   * @param index
   */
  def writeIndex(indexName: IndexName, index: DataFrame): Unit

  /**
   *
   * @param indexName
   */
  def dropIndex(indexName : IndexName) : Unit
}
