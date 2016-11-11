package org.vitrivr.adampro.api

import org.vitrivr.adampro.entity.Entity._
import org.vitrivr.adampro.entity.{AttributeDefinition, Entity, EntityPartitioner}
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.helpers.benchmark.ScanWeightCatalogOperator
import org.vitrivr.adampro.helpers.partition.PartitionMode
import org.vitrivr.adampro.helpers.sparsify.SparsifyHelper
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.query.Predicate
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
  def apply(entityname: EntityName, fields: Seq[AttributeDefinition])(implicit ac: AdamContext): Try[Entity] = {
    create(entityname, fields)(ac)
  }

  /**
    * Creates an entity.
    *
    * @param entityname  name of entity
    * @param fields      fields of the entity to create
    * @param ifNotExists if set to true and the entity exists, the entity is just returned; otherwise an error is thrown
    * @return
    */
  def create(entityname: EntityName, fields: Seq[AttributeDefinition], ifNotExists: Boolean = false)(implicit ac: AdamContext): Try[Entity] = {
    execute("create entity " + entityname + " operation") {
      Entity.create(entityname, fields, ifNotExists)
    }
  }

  /**
    * Checks if entity exists
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
    * Caches entity.
    *
    * @param entityname name of entity
    * @return
    */
  def cache(entityname: EntityName)(implicit ac: AdamContext): Try[Void] = {
    execute("cache entity " + entityname + " operation") {
      Entity.load(entityname).get.cache()
      Success(null)
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
    * Compress operation. Compresses dense vectors to sparse vectors.
    *
    * @param entityname name of entity
    * @return
    */
  def sparsify(entityname: EntityName, attribute: String)(implicit ac: AdamContext): Try[Entity] = {
    execute("compress tuples in entity " + entityname + " operation") {
      SparsifyHelper(Entity.load(entityname).get, attribute)
    }
  }

  /**
    * Inserts data into the entity.
    *
    * @param entityname name of entity
    * @param df         data frame containing all attributes (of both the feature storage and the metadata storage)
    *
    */
  def insert(entityname: EntityName, df: DataFrame)(implicit ac: AdamContext): Try[Void] = {
    execute("insert data into entity " + entityname + " operation") {
      Entity.load(entityname).get.insert(df)
    }
  }

  /**
    * Deletes data from the entity.
    *
    * @param entityname name of entity
    * @param predicates list of predicates
    *
    */
  def delete(entityname: EntityName, predicates : Seq[Predicate])(implicit ac: AdamContext): Try[Int] = {
    execute("delete data from " + entityname + " operation") {
      Success(Entity.load(entityname).get.delete(predicates))
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
      Success(Entity.load(entityname).get.show(k).get)
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
      Success(Entity.load(entityname).get.propertiesMap)
    }
  }

  /**
    * Repartitions the entity.
    *
    * @param entityname  name of entity
    * @param nPartitions number of partitions
    * @param cols        attributes to partition after
    * @param mode        partition mode (e.g., create new index, replace current index, etc.)
    * @return
    */
  def partition(entityname: EntityName, nPartitions: Int, joins: Option[DataFrame], cols: Option[Seq[String]], mode: PartitionMode.Value)(implicit ac: AdamContext): Try[Entity] = {
    execute("repartition entity " + entityname + " operation") {
      EntityPartitioner(Entity.load(entityname).get, nPartitions, joins, cols, mode)
    }
  }

  /**
    * Sets the weight of the entity to make it more important in the search
    *
    * @param entityname name of entity
    * @param attribute     name of attribute
    * @param weight     new weight to set (the higher, the more important the index is)
    * @return
    */
  def setScanWeight(entityname: EntityName, attribute: String, weight: Float)(implicit ac: AdamContext): Try[Void] = {
    execute("set entity weight for " + entityname + "(" + attribute + ")" + " operation") {
      ScanWeightCatalogOperator.set(Entity.load(entityname).get, attribute, weight)
      Success(null)
    }
  }

  /**
    * Resets all scan weights entity and indexes corresponding to entity.
    *
    * @param entityname name of entity
    */
  def resetScanWeights(entityname: EntityName)(implicit ac: AdamContext): Try[Void] = {
    execute("reset weights of entity and indexes of " + entityname) {
      throw new GeneralAdamException("not implemented yet")
      Success(null)
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
