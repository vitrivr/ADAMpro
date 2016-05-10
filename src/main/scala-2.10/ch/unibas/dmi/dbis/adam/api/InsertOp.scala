package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
object InsertOp {
  val log = Logger.getLogger(getClass.getName)

  /**
    *
    * @param entityname
    * @param df data frame containing all columns (of both the feature storage and the metadata storage);
    *           note that you should name the feature column as ("feature").
    */
  def apply(entityname: EntityName, df: DataFrame)(implicit ac: AdamContext): Try[Void] = {
    try {
      log.debug("perform insert data operation")
      EntityHandler.insertData(entityname, df)
      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}
