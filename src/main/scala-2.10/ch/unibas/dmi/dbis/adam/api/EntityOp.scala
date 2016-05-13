package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.{Entity, FieldDefinition}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
object EntityOp extends Logging {
  /**
    * Lists names of all entities.
    *
    * @return
    */
  def list(): Try[Seq[EntityName]] = {
    try {
      log.debug("perform list entities operation")
      Success(Entity.list)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Creates an entity.
    *
    * @param entityname name of entity
    * @param fields     fields of the entity to create
    * @return
    */
  def apply(entityname: EntityName, fields: Seq[FieldDefinition])(implicit ac: AdamContext): Try[Entity] = {
    try {
      log.debug("perform create entity operation")
      Entity.create(entityname, fields)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Checks if index exists
    *
    * @param entityname name of entity
    * @return
    */
  def exists(entityname: EntityName)(implicit ac: AdamContext): Try[Boolean] = {
    try {
      log.debug("perform entity exists operation")
      Success(Entity.exists(entityname))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Count operation. Returns number of elements in entity (only feature storage is considered).
    *
    * @param entityname name of entity
    * @return
    */
  def count(entityname: EntityName)(implicit ac: AdamContext): Try[Long] = {
    try {
      log.debug("perform count operation")
      Success(Entity.load(entityname).get.count)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Inserts data into the entity.
    *
    * @param entityname name of entity
    * @param df         data frame containing all columns (of both the feature storage and the metadata storage)
    *
    */
  def insert(entityname: EntityName, df: DataFrame)(implicit ac: AdamContext): Try[Void] = {
    try {
      log.debug("perform insert data operation")
      Entity.load(entityname).get.insert(df)
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    * Gives preview of entity.
    *
    * @param entityname name of entity
    * @param k          number of elements to show in preview
    * @return
    */
  def preview(entityname: EntityName, k: Int = 100)(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      log.debug("perform preview entity operation")
      Success(Entity.load(entityname).get.show(k))
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    * Returns properties of entity.
    *
    * @param entityname name of entity
    * @return
    */
  def properties(entityname: EntityName)(implicit ac: AdamContext): Try[Map[String, String]] = {
    try {
      log.debug("perform properties operation")
      Success(Entity.load(entityname).get.properties)
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    * Drops an entity.
    *
    * @param entityname name of entity
    * @param ifExists   returns no error if set to true and entity does not exist
    * @return
    */
  def drop(entityname: EntityName, ifExists: Boolean = false)(implicit ac: AdamContext): Try[Void] = {
    try {
      log.debug("perform drop entity operation")
      Entity.drop(entityname, ifExists)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}
