package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.AdamContext
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
    * @param indexname
    * @return
    */
  def exists(indexname : IndexName) : Boolean

  /**
    * Create an index in the index storage.
    *
    * @param indexname
    * @param df
    * @return
    */
  def create(indexname: IndexName, df: DataFrame)(implicit ac: AdamContext) = write(indexname, df)

  /**
    * Read index from the index storage.
    *
    * @param indexName
    * @return
    */
  def read(indexName: IndexName)(implicit ac : AdamContext): DataFrame

  /**
    * Write index to the index storage.
    *
    * @param indexName
    * @param index
    * @return true on success
    */
  def write(indexName: IndexName, index: DataFrame)(implicit ac: AdamContext): Boolean

  /**
    * Drop the index from the index storage.
    *
    * @param indexName
    * @return true on success
    */
  def drop(indexName: IndexName)(implicit ac: AdamContext): Boolean
}
