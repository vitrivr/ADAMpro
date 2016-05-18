package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
trait IndexStorage extends Serializable with Logging {
  /**
    *
    * @param indexname
    * @return
    */
  def exists(indexname : IndexName) : Try[Boolean]

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
  def read(indexName: IndexName)(implicit ac : AdamContext): Try[DataFrame]

  /**
    * Write index to the index storage.
    *
    * @param indexName
    * @param index
    * @return true on success
    */
  def write(indexName: IndexName, index: DataFrame)(implicit ac: AdamContext): Try[Void]

  /**
    * Drop the index from the index storage.
    *
    * @param indexName
    * @return true on success
    */
  def drop(indexName: IndexName)(implicit ac: AdamContext): Try[Void]
}
