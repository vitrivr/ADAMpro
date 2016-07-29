package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.helpers.scanweight.ScanWeightInspector
import ch.unibas.dmi.dbis.adam.index.{IndexPartitioner, Index}
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.helpers.partition.{PartitionMode, PartitionerChoice}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

/**
  * adamtwo
  *
  *
  * Ivan Giangreco
  * August 2015
  */
object IndexOp extends GenericOp {


  /**
    * Lists all indexes.
    *
    * @param entityname name of entity
    */
  def list(entityname: EntityName)(implicit ac: AdamContext): Try[Seq[(IndexName, IndexTypeName)]] = {
    execute("list indexes for " + entityname) {
      Success(Entity.load(entityname).get.indexes.filter(_.isSuccess).map(_.get).map(index => (index.indexname, index.indextypename)))
    }
  }

  /**
    * Creates an index.
    *
    * @param entityname    name of entity
    * @param attribute     name of attribute
    * @param indextypename index type to use for indexing
    * @param distance      distance function to use
    * @param properties    further index specific properties
    */
  def apply(entityname: EntityName, attribute: String, indextypename: IndexTypeName, distance: DistanceFunction, properties: Map[String, String] = Map())(implicit ac: AdamContext): Try[Index] = {
    create(entityname, attribute, indextypename, distance, properties)
  }

  /**
    * Creates an index.
    *
    * @param entityname    name of entity
    * @param attribute     name of attribute
    * @param indextypename index type to use for indexing
    * @param distance      distance function to use
    * @param properties    further index specific properties
    */
  def create(entityname: EntityName, attribute: String, indextypename: IndexTypeName, distance: DistanceFunction, properties: Map[String, String] = Map())(implicit ac: AdamContext): Try[Index] = {
    execute("create index for " + entityname) {
      Index.createIndex(Entity.load(entityname).get, attribute, indextypename.indexer(distance, properties, ac))
    }
  }

  /**
    * Creates indexes of all available types.
    *
    * @param entityname name of entity
    * @param attribute  name of attribute
    * @param distance   distance function to use
    * @param properties further index specific properties
    */
  def generateAll(entityname: EntityName, attribute: String, distance: DistanceFunction, properties: Map[String, String] = Map())(implicit ac: AdamContext): Try[Seq[IndexName]] = {
    execute("create all indexes for " + entityname) {
      val indexes = IndexTypes.values.map {
        apply(entityname, attribute, _, distance, properties)
      }

      //check and possibly clean up
      if (indexes.forall(_.isSuccess)) {
        //all indexes were created, return
        return Success(indexes.map(_.get.indexname))
      }

      log.error("not all indexes were created")

      //not all indexes were created, delete the ones that were successfull too
      indexes
        .filter(_.isSuccess)
        .map(_.get.indexname)
        .foreach {
          indexname =>
            Index.drop(indexname)
        }

      return Failure(new GeneralAdamException("some indexes were not created properly."))
    }
  }

  /**
    * Checks if index exists
    *
    * @param indexname name of index
    * @return
    */
  def exists(indexname: IndexName)(implicit ac: AdamContext): Try[Boolean] = {
    execute("check index " + indexname + " exists operation") {
      Success(Index.exists(indexname))
    }
  }


  /**
    * Checks if index exists
    *
    * @param entityname    name of entity
    * @param attribute     name of attribute
    * @param indextypename index type to use for indexing
    * @return
    */
  def exists(entityname: EntityName, attribute: String, indextypename: IndexTypeName)(implicit ac: AdamContext): Try[Boolean] = {
    execute("check index for " + entityname + "(" + attribute + ")" + " of type " + indextypename + " exists operation") {
      Success(Index.exists(entityname, attribute, indextypename))
    }
  }


  /**
    * Sets the weight of the index to make it more important in the search
    *
    * @param indexname name of index
    * @param weight    new weight to set (the higher, the more important the index is)
    * @return
    */
  def setScanWeight(indexname: IndexName, weight: Float)(implicit ac: AdamContext): Try[Void] = {
    execute("set index weight for " + indexname + " operation") {
      ScanWeightInspector.set(Index.load(indexname).get, weight)
      Success(null)
    }
  }

  /**
    * Loads the index into cache for faster processing.
    *
    * @param indexname name of index
    * @return
    */
  def cache(indexname: IndexName)(implicit ac: AdamContext): Try[Index] = {
    execute("cache index " + indexname + " operation") {
      Index.load(indexname, cache = true)
    }
  }

  /**
    * Repartitions the index.
    *
    * @param indexname   name of index
    * @param nPartitions number of partitions
    * @param attributes  attributes to partition after
    * @param mode        partition mode (e.g., create new index, replace current index, etc.)
    * @return
    */
  def partition(indexname: IndexName, nPartitions: Int, joins: Option[DataFrame], attributes: Option[Seq[String]], mode: PartitionMode.Value, partitioner: PartitionerChoice.Value = PartitionerChoice.SPARK, options: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): Try[Index] = {
    execute("repartition index " + indexname + " operation") {
      IndexPartitioner(Index.load(indexname).get, nPartitions, joins, attributes, mode, partitioner, options)
    }
  }

  /**
    * Drops an index.
    *
    * @param indexname name of index
    * @return
    */
  def drop(indexname: IndexName)(implicit ac: AdamContext): Try[Void] = {
    execute("drop index " + indexname + " operation") {
      Index.drop(indexname)
    }
  }
}
