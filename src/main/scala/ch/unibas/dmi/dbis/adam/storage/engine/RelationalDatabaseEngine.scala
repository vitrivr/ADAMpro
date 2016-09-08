package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{SaveMode, DataFrame}

import scala.util.{Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
trait RelationalDatabaseEngine extends Serializable with Logging {
  /**
    * Create entity in database storage.
    *
    * @param tablename  name of table
    * @param attributes attributes
    */
  def create(tablename: String, attributes: Seq[AttributeDefinition])(implicit ac: AdamContext): Try[Option[String]] = Success(None)

  /**
    *
    * @param tablename name of table
    * @return
    */
  def exists(tablename: String)(implicit ac: AdamContext): Try[Boolean]

  /**
    * Read data from database storage.
    *
    * @param tablename name of table
    * @return
    */
  def read(tablename: String, where : Option[Seq[String]] = None)(implicit ac: AdamContext): Try[DataFrame]

  /**
    * Write data to database storage.
    *
    * @param tablename name of table
    * @param data      data
    * @return
    */
  def write(tablename: String, data: DataFrame, mode : SaveMode = SaveMode.Append)(implicit ac: AdamContext): Try[Void]

  /**
    * Drop data from the database storage.
    *
    * @param tablename name of table
    * @return
    */
  def drop(tablename: String)(implicit ac: AdamContext): Try[Void]
}
