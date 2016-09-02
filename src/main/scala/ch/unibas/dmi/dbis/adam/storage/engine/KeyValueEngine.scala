package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2016
  */
trait KeyValueEngine  extends Serializable with Logging {
  /**
    * Create the entity in the key-value store.
    *
    * @param bucketname name of bucket to store feature to
    * @param attributes attributes of the entity
    * @return true on success
    */
  def create(bucketname: String, attributes: Seq[AttributeDefinition])(implicit ac: AdamContext): Try[Option[String]]

  /**
    *
    * @param bucketname name of bucket to check if bucket exists
    * @return
    */
  def exists(bucketname: String)(implicit ac: AdamContext): Try[Boolean]

  /**
    * Read entity from key-value store.
    *
    * @param bucketname name of bucket to read features from
    * @return
    */
  def read(bucketname: String)(implicit ac: AdamContext): Try[DataFrame]

  /**
    * Write entity to the key-value store.
    *
    * @param bucketname name of bucket to write features to
    * @param df   data
    * @param mode save mode (append, overwrite, ...)
    * @return true on success
    */
  def write(bucketname: String, df: DataFrame, mode: SaveMode = SaveMode.Append)(implicit ac: AdamContext): Try[Void]

  /**
    * Drop the entity from the key-value store.
    *
    * @param bucketname name of bucket to be dropped
    * @return true on success
    */
  def drop(bucketname: String)(implicit ac: AdamContext): Try[Void]
}
