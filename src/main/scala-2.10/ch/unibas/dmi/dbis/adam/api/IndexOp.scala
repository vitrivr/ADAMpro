package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator, IndexHandler}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.log4j.Logger

import scala.util.{Success, Failure, Try}

/**
  * adamtwo
  *
  * Index operation. Creates an index.
  *
  * Ivan Giangreco
  * August 2015
  */
object IndexOp {
  val log = Logger.getLogger(getClass.getName)

  /**
    * Creates an index.
    *
    * @param entityname
    * @param indextype  string representation of index type to use for indexing
    * @param distance   distance function to use
    * @param properties further index specific properties
    */
  def apply(entityname: EntityName, column: String, indextype: String, distance: DistanceFunction, properties: Map[String, String])(implicit ac: AdamContext): Try[Index] = {
    try {
      apply(entityname, column, IndexTypes.withName(indextype).get, distance, properties)
    } catch {
      case e : Exception => Failure(e)

    }
  }

  /**
    * Creates an index.
    *
    * @param entityname
    * @param indextypename index type to use for indexing
    * @param distance      distance function to use
    * @param properties    further index specific properties
    */
  def apply(entityname: EntityName, column: String, indextypename: IndexTypeName, distance: DistanceFunction, properties: Map[String, String] = Map())(implicit ac: AdamContext): Try[Index] = {
    try {
      log.debug("perform create index operation")

      val entity = EntityHandler.load(entityname)

      val generator: IndexGenerator = indextypename.indexer(distance, properties)

      IndexHandler.createIndex(entity.get, column, generator)
    } catch {
      case e : Exception => Failure(e)
    }
  }

  /**
    * Creates indexes of all available types.
    *
    * @param entityname
    * @param distance   distance function to use
    * @param properties further index specific properties
    */
  def generateAll(entityname: EntityName, column: String, distance: DistanceFunction, properties: Map[String, String] = Map())(implicit ac: AdamContext): Try[Boolean] = {
    try {
      log.debug("perform generate all indexes operation")

      val indexes = IndexTypes.values.map {
        indextypename =>
          apply(entityname, column, indextypename, distance, properties)
      }

      if (indexes.forall(_.isSuccess)) {
        //all indexes were created
        return Success(true)
      } else {
        //reset indexes: delete successfull ones
        indexes
          .filter(_.isSuccess)
          .map(_.get.indexname)
          .foreach {
            indexname =>
              IndexHandler.drop(indexname)
          }

        return Failure(throw new GeneralAdamException())
      }
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    *
    * @param indexname
    * @param weight
    * @return
    */
  def setWeight(indexname: IndexName, weight: Float)(implicit ac: AdamContext): Try[Boolean] = {
    try {
      Success(IndexHandler.setWeight(indexname, weight))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    *
    * @param indexname
    * @return
    */
  def cache(indexname: IndexName)(implicit ac: AdamContext): Try[Index] = {
    try {
      IndexHandler.load(indexname, true)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}
