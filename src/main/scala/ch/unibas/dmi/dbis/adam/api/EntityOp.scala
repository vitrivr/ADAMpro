package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.{Entity, FieldDefinition}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.spark.sql.DataFrame

import scala.util.{Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
object EntityOp extends GenericOp {
  /**
    * Lists names of all entities.
    *
    * @return
    */
  def list(): Try[Seq[EntityName]] = {
    execute("list entities operation") {
      Success(Entity.list)
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
    execute("create entity " + entityname + " operation") {
      Entity.create(entityname, fields)
    }
  }

  /**
    * Checks if index exists
    *
    * @param entityname name of entity
    * @return
    */
  def exists(entityname: EntityName)(implicit ac: AdamContext): Try[Boolean] = {
    execute("check entity " + entityname + " exists operation") {
      Success(Entity.exists(entityname))
    }
  }

  /**
    * Count operation. Returns number of elements in entity (only feature storage is considered).
    *
    * @param entityname name of entity
    * @return
    */
  def count(entityname: EntityName)(implicit ac: AdamContext): Try[Long] = {
    execute("count tuples in entity " + entityname + " operation") {
      Success(Entity.load(entityname).get.count)
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
    execute("insert data into entity " + entityname + " operation") {
      Entity.load(entityname).get.insert(df)
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
    execute("preview entity " + entityname + " operation") {
      Success(Entity.load(entityname).get.show(k))
    }
  }


  /**
    * Returns properties of entity.
    *
    * @param entityname name of entity
    * @return
    */
  def properties(entityname: EntityName)(implicit ac: AdamContext): Try[Map[String, String]] = {
    execute("load properties of entity " + entityname + " operation") {
      Success(Entity.load(entityname).get.properties)
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
    execute("drop entity " + entityname + " operation") {
      Entity.drop(entityname, ifExists)
    }
  }
}
