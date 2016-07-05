package ch.unibas.dmi.dbis.adam.storage.handler

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
trait StorageHandler extends Logging with Serializable {
  val name: String

  def supports: Seq[FieldType]
  def specializes : Seq[FieldType]

  //specializes should be contained in supports
  assert(specializes.forall(supports.contains(_)))


  /**
    * Executes operation.
    *
    * @param desc description to display in log
    * @param op   operation to perform
    * @return
    */
  protected def execute[T](desc: String)(op: => Try[T]): Try[T] = {
    try {
      log.trace("performed storage handler (" + name + ") operation: " + desc)
      op
    } catch {
      case e: Exception =>
        log.error("error in storage handler (" + name + ") operation: " + desc, e)
        Failure(e)
    }
  }

  def create(entityname: EntityName, attributes: Seq[AttributeDefinition], params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Void]

  def read(entityname: EntityName, params : Map[String, String] = Map())(implicit ac: AdamContext): Try[DataFrame]

  def write(entityname: EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append, params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Void]

  def drop(entityname: EntityName, params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Void]

  override def equals(that: Any): Boolean

  override def hashCode: Int
}
