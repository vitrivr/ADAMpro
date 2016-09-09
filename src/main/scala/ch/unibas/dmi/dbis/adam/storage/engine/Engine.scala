package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
trait Engine extends Serializable with Logging {
  val name: String

  def supports: Seq[FieldType]

  def specializes: Seq[FieldType]

  //specializes should be contained in supports
  assert(specializes.forall(supports.contains(_)))


  /**
    * Create the entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes attributes of the entity (w.r.t. handler)
    * @param params      creation parameters
    * @return options to store
    */
  def create(storename: String, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: AdamContext): Try[Map[String, String]]

  /**
    * Check if entity exists.
    *
    * @param storename  adapted entityname to store feature to
    * @return
    */
  def exists(storename: String)(implicit ac: AdamContext): Try[Boolean]

  /**
    * Read entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param params      reading parameters
    * @return
    */
  def read(storename: String, params: Map[String, String])(implicit ac: AdamContext): Try[DataFrame]

  /**
    * Write entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param df         data
    * @param mode       save mode (append, overwrite, ...)
    * @param params      writing parameters
    * @return new options to store
    */
  def write(storename: String, df: DataFrame, mode: SaveMode = SaveMode.Append, params: Map[String, String])(implicit ac: AdamContext): Try[Map[String, String]]

  /**
    * Drop the entity.
    *
    * @param storename  adapted entityname to store feature to
    * @return
    */
  def drop(storename: String)(implicit ac: AdamContext): Try[Void]
}
