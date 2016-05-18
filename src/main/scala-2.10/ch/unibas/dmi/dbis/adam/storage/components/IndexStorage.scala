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
    * @param path
    * @return
    */
  def exists(path : String) : Try[Boolean]

  /**
    * Create an index in the index storage.
    *
    * @param indexname
    * @param df
    * @return
    */
  def create(indexname: IndexName, df: DataFrame)(implicit ac: AdamContext) : Try[String] = write(indexname, df)

  /**
    * Read index from the index storage.
    *
    * @param path
    * @return
    */
  def read(path : String)(implicit ac : AdamContext): Try[DataFrame]

  /**
    * Write index to the index storage.
    *
    * @param indexName
    * @param index
    * @return true on success
    */
  def write(indexName: IndexName, index: DataFrame, path : Option[String] = None)(implicit ac: AdamContext): Try[String]

  /**
    * Drop the index from the index storage.
    *
    * @param path
    * @return true on success
    */
  def drop(path : String)(implicit ac: AdamContext): Try[Void]
}
