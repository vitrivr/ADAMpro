package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.api.IndexOp._
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.Entity.create
import ch.unibas.dmi.dbis.adam.entity.{Entity, AttributeDefinition}
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.scanweight.{ScanWeightHandler, ScanWeightHandler$}
import ch.unibas.dmi.dbis.adam.storage.partition.PartitionMode
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
      Success(Entity.load(entityname).get.properties)
    }
  }

  /**
    * Repartitions the entity.
    *
    * @param entityname  name of entity
    * @param nPartitions number of partitions
    * @param cols        columns to partition after
    * @param mode        partition mode (e.g., create new index, replace current index, etc.)
    * @return
    */
  def partition(entityname: EntityName, nPartitions: Int, joins: Option[DataFrame], cols: Option[Seq[String]], mode: PartitionMode.Value)(implicit ac: AdamContext): Try[Entity] = {
    execute("repartition entity " + entityname + " operation") {
      Entity.repartition(Entity.load(entityname).get, nPartitions, joins, cols, mode)
    }
  }

  /**
    * Benchmarks entity and indexes corresponding to entity and adjusts scan weights.
    *
    * @param entityname name of entity
    * @param column     name of column
    * @return
    */
  def benchmarkAndSetScanWeights(entityname: EntityName, column: String)(implicit ac: AdamContext): Try[Void] = {
    execute("benchmark entity and indexes of " + entityname + "(" + column + ")" + "and adjust weights operation") {
      val swh = new ScanWeightHandler(entityname, column)
      swh.benchmarkAndUpdate()
      Success(null)
    }
  }

  /**
    * Sets the weight of the entity to make it more important in the search
    *
    * @param entityname name of entity
    * @param column     name of column
    * @param weight    new weight to set (the higher, the more important the index is)
    * @return
    */
  def setScanWeight(entityname: EntityName, column : String, weight: Float)(implicit ac: AdamContext): Try[Void] = {
    execute("set entity weight for " + entityname + "(" + column +")" + " operation") {
      Entity.load(entityname).get.setScanWeight(column, Some(weight))
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
      ScanWeightHandler.resetWeights(entityname)
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
