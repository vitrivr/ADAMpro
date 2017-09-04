package org.vitrivr.adampro.communication.api

import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.data.entity.Entity._
import org.vitrivr.adampro.data.entity.{AttributeDefinition, Entity, EntityPartitioner, SparsifyHelper}
import org.vitrivr.adampro.distribution.partitioning.{PartitioningManager, PartitionMode}
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.query.Predicate

import scala.util.{Failure, Success, Try}

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
  def list()(implicit ac: SharedComponentContext): Try[Seq[EntityName]] = {
    execute("list entities operation") {
      Success(Entity.list)
    }
  }

  /**
    * Creates an entity.
    *
    * @param entityname  name of entity
    * @param attributes  fields of the entity to create
    * @param ifnotexists if set to true and the entity exists, the entity is just returned; otherwise an error is thrown
    * @return
    */
  def create(entityname: EntityName, attributes: Seq[AttributeDefinition], ifnotexists: Boolean = false)(implicit ac: SharedComponentContext): Try[Entity] = {
    execute("create entity " + entityname + " operation") {
      Entity.create(entityname, attributes, ifnotexists)
    }
  }

  /**
    * Checks if entity exists
    *
    * @param entityname name of entity
    * @return
    */
  def exists(entityname: EntityName)(implicit ac: SharedComponentContext): Try[Boolean] = {
    execute("check entity " + entityname + " exists operation") {
      Success(Entity.exists(entityname))
    }
  }


  /**
    * Caches entity.
    *
    * @param entityname name of entity
    * @return
    */
  def cache(entityname: EntityName)(implicit ac: SharedComponentContext): Try[Void] = {
    execute("cache entity " + entityname + " operation") {
      val entity = Entity.load(entityname)

      if (entity.isSuccess) {
        entity.get.cache()
        Success(null)
      } else {
        Failure(entity.failed.get)
      }
    }
  }

  /**
    * Count operation. Returns number of elements in entity (only feature storage is considered).
    *
    * @param entityname name of entity
    * @return
    */
  def count(entityname: EntityName)(implicit ac: SharedComponentContext): Try[Long] = {
    execute("count tuples in entity " + entityname + " operation") {
      val entity = Entity.load(entityname)

      if (entity.isSuccess) {
        Success(entity.get.count)
      } else {
        Failure(entity.failed.get)
      }
    }
  }

  /**
    * Compresses dense vectors to sparse vectors.
    *
    * @param entityname name of entity
    * @return
    */
  def sparsify(entityname: EntityName, attribute: String)(implicit ac: SharedComponentContext): Try[Entity] = {
    execute("compress tuples in entity " + entityname + " operation") {
      val entity = Entity.load(entityname)

      if (entity.isSuccess) {
        SparsifyHelper(entity.get, attribute)
      } else {
        Failure(entity.failed.get)
      }
    }
  }

  /**
    * Inserts data into the entity.
    *
    * @param entityname name of entity
    * @param inserts    data frame containing all attributes to insert
    *
    */
  def insert(entityname: EntityName, inserts: DataFrame)(implicit ac: SharedComponentContext): Try[Void] = {
    execute("insert data into entity " + entityname + " operation") {
      val entity = Entity.load(entityname)

      if (entity.isSuccess) {
        entity.get.insert(inserts)
      } else {
        Failure(entity.failed.get)
      }
    }
  }

  /**
    * Deletes data from the entity.
    *
    * @param entityname name of entity
    * @param predicates list of predicates
    *
    */
  def delete(entityname: EntityName, predicates: Seq[Predicate])(implicit ac: SharedComponentContext): Try[Int] = {
    execute("delete data from " + entityname + " operation") {
      val entity = Entity.load(entityname)

      if (entity.isSuccess) {
        Success(entity.get.delete(predicates))
      } else {
        Failure(entity.failed.get)
      }
    }
  }

  /**
    * Vacuum an entity.
    *
    * @param entityname name of entity
    *
    */
  def vacuum(entityname: EntityName)(implicit ac: SharedComponentContext): Try[Void] = {
    execute("vacuum entity " + entityname + " operation") {
      val entity = Entity.load(entityname)

      if (entity.isSuccess) {
        entity.get.vacuum()
        Success(null)
      } else {
        Failure(entity.failed.get)
      }
    }
  }


  /**
    * Gives preview of entity.
    *
    * @param entityname name of entity
    * @param k          number of elements to show in preview
    * @return
    */
  def preview(entityname: EntityName, k: Int = 100)(implicit ac: SharedComponentContext): Try[DataFrame] = {
    execute("preview entity " + entityname + " operation") {
      val entity = Entity.load(entityname)

      if (entity.isSuccess) {
        Success(entity.get.show(k).get)
      } else {
        Failure(entity.failed.get)
      }
    }
  }


  /**
    * Returns properties of an entity/attribute in an entity.
    *
    * @param entityname name of entity
    * @param attribute  name of attribute
    * @param options    possible options for operation
    * @return
    */
  def properties(entityname: EntityName, attribute: Option[String] = None, options: Map[String, String] = Map())(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    execute("load properties of entity " + entityname + " operation") {
      val entity = Entity.load(entityname)

      if (entity.isSuccess) {
        if(attribute.isDefined) {
          Success(entity.get.attributePropertiesMap(attribute.get, options))
        } else {
          Success(entity.get.propertiesMap(options))
        }
      } else {
        Failure(entity.failed.get)
      }
    }
  }

  /**
    * Repartitions the entity.
    *
    * @param entityname  name of entity
    * @param npartitions number of partitions
    * @param attribute   name of attribute to use as partitioning key
    * @param mode        partition mode (e.g., create new index, replace current index, etc.)
    * @return
    */
  def partition(entityname: EntityName, npartitions: Int, joins: Option[DataFrame], attribute: Option[AttributeName], mode: PartitionMode.Value)(implicit ac: SharedComponentContext): Try[Entity] = {
    execute("repartition entity " + entityname + " operation") {
      val entity = Entity.load(entityname)

      if (entity.isSuccess) {
        PartitioningManager.fragment(entity.get, npartitions, joins, attribute, mode)
      } else {
        Failure(entity.failed.get)
      }
    }
  }

  /**
    * Drops an entity.
    *
    * @param entityname name of entity
    * @param ifexists   returns no error if set to true and entity does not exist
    * @return
    */
  def drop(entityname: EntityName, ifexists: Boolean = false)(implicit ac: SharedComponentContext): Try[Void] = {
    execute("drop entity " + entityname + " operation") {
      Entity.drop(entityname, ifexists)
    }
  }
}
