package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.spark.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
trait FeatureStorage extends Serializable with Logging {
  /**
    *
    * @param path path to store features to
    * @return
    */
  def exists(path: String): Try[Boolean]

  /**
    * Create the entity in the feature storage.
    *
    * @param entityname name of entity
    * @return true on success
    */
  def create(entityname: EntityName, fields: Seq[AttributeDefinition])(implicit ac: AdamContext): Try[Option[String]] = Success(None)

  /**
    * Read entity from feature storage.
    *
    * @param path path to store features to
    * @return
    */
  def read(path: String)(implicit ac: AdamContext): Try[DataFrame]

  /**
    * Count the number of tuples in the feature storage.
    *
    * @param path path to store features to
    * @return
    */
  def count(path: String)(implicit ac: AdamContext): Try[Long]

  /**
    * Write entity to the feature storage.
    *
    * @param path path to store features to
    * @param df   data
    * @param mode save mode (append, overwrite, ...)
    * @return true on success
    */
  def write(entityname: EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append, path: Option[String] = None, allowRepartitioning: Boolean = false)(implicit ac: AdamContext): Try[String]

  /**
    * Drop the entity from the feature storage
    *
    * @param path path to store features to
    * @return true on success
    */
  def drop(path: String)(implicit ac: AdamContext): Try[Void]
}

