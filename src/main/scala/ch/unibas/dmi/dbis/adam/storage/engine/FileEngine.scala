package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
trait FileEngine extends Serializable with Logging {
   /**
    * Create the entity in the file system.
    *
    * @param filename relative filename to store feature to
    * @param attributes attributes of the entity
    * @return true on success
    */
  def create(filename: String, attributes: Seq[AttributeDefinition])(implicit ac: AdamContext): Try[Option[String]] = Success(None)

  /**
    *
    * @param filename relative filename to check if file exists
    * @return
    */
  def exists(filename: String)(implicit ac: AdamContext): Try[Boolean]

  /**
    * Read entity from file system.
    *
    * @param filename relative filename to read features from
    * @return
    */
  def read(filename: String)(implicit ac: AdamContext): Try[DataFrame]

  /**
    * Write entity to the file system.
    *
    * @param filename relative filename to store features to
    * @param df   data
    * @param mode save mode (append, overwrite, ...)
    * @param allowRepartitioning whether to allow re-partitiniong of data when saving
    * @return true on success
    */
  def write(filename: String, df: DataFrame, mode: SaveMode = SaveMode.Append, allowRepartitioning: Boolean = false)(implicit ac: AdamContext): Try[Void]

  /**
    * Drop the entity from the file system.
    *
    * @param filename relative filename to be dropped
    * @return true on success
    */
  def drop(filename: String)(implicit ac: AdamContext): Try[Void]
}

