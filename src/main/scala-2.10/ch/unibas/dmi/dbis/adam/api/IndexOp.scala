package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.storage.partition.PartitionMode
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

/**
  * adamtwo
  *
  *
  * Ivan Giangreco
  * August 2015
  */
object IndexOp extends APIHandler {

  /**
    * Creates an index.
    *
    * @param entityname name of entity
    * @param indextype  string representation of index type to use for indexing
    * @param distance   distance function to use
    * @param properties further index specific properties
    */
  def apply(entityname: EntityName, column: String, indextype: String, distance: DistanceFunction, properties: Map[String, String])(implicit ac: AdamContext): Try[Index] = apply(entityname, column, IndexTypes.withName(indextype).get, distance, properties)

  /**
    * Creates an index.
    *
    * @param entityname    name of entity
    * @param indextypename index type to use for indexing
    * @param distance      distance function to use
    * @param properties    further index specific properties
    */
  def apply(entityname: EntityName, column: String, indextypename: IndexTypeName, distance: DistanceFunction, properties: Map[String, String] = Map())(implicit ac: AdamContext): Try[Index] = {
    execute("create index for " + entityname) {
      Index.createIndex(Entity.load(entityname).get, column, indextypename.indexer(distance, properties))
    }
  }

  /**
    * Creates indexes of all available types.
    *
    * @param entityname name of entity
    * @param distance   distance function to use
    * @param properties further index specific properties
    */
  def generateAll(entityname: EntityName, column: String, distance: DistanceFunction, properties: Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    execute("create all indexes for " + entityname) {
      val indexes = IndexTypes.values.map {
        apply(entityname, column, _, distance, properties)
      }

      //check and possibly clean up
      if (indexes.forall(_.isSuccess)) {
        //all indexes were created, return
        return Success(null)
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
    * Sets the weight of the index to make it more important in the search
    *
    * @param indexname name of index
    * @param weight    new weight to set (the higher, the more important the index is)
    * @return
    */
  def setWeight(indexname: IndexName, weight: Float)(implicit ac: AdamContext): Try[Void] = {
    execute("set index weight for " + indexname + " operation") {
      Index.load(indexname).get.setWeight(weight)
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
      Index.load(indexname, true)
    }
  }

  /**
    * Repartitions the index.
    *
    * @param indexname   name of index
    * @param nPartitions number of partitions
    * @param cols        columns to partition after
    * @param mode        partition mode (e.g., create new index, replace current index, etc.)
    * @return
    */
  def partition(indexname: IndexName, nPartitions: Int, joins: Option[DataFrame], cols: Option[Seq[String]], mode: PartitionMode.Value)(implicit ac: AdamContext): Try[Index] = {
    execute("repartition index " + indexname + " operation") {
      Index.repartition(Index.load(indexname).get, nPartitions, joins, cols, mode)
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
