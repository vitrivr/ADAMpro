package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.datatypes.Feature
import Feature._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, NormBasedDistanceFunction}
import ch.unibas.dmi.dbis.adam.storage.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.table.Table.TableName

import scala.collection.mutable.{Map => mMap}

import scala.concurrent._
import ExecutionContext.Implicits.global


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object QueryHandler {
  private val storage = SparkStartup.tableStorage

  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param tablename
   * @return
   */
  def sequentialQuery(q: WorkingVector, distance : DistanceFunction, k : Int, tablename: TableName): Seq[Result] = {
    TableScanner(q, distance, k, tablename)
  }

  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param tablename
   * @return
   */
  def sequentialQueryNonBlocking(q: WorkingVector, distance : DistanceFunction, k : Int, tablename: TableName): Future[Seq[Result]] = {
    Future{
      TableScanner(q, distance, k, tablename)
    }
  }

  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param indexname
   * @param options
   * @return
   */
  def indexQuery(q: WorkingVector, distance : DistanceFunction, k : Int, indexname : IndexName, options : Map[String, String]): Seq[Result] = {
    val tablename = CatalogOperator.getIndexTableName(indexname)
    val tidList = IndexScanner(q, distance, k, indexname, options)
    TableScanner(q, distance, k, tablename, tidList)
  }

  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param indexname
   * @param options
   * @return
   */
  def indexQueryNonBlocking(q: WorkingVector, distance : DistanceFunction, k : Int, indexname : IndexName, options : Map[String, String]): Future[Seq[Result]] = {
    Future {
      val tablename = CatalogOperator.getIndexTableName(indexname)
      val tidList = IndexScanner(q, distance, k, indexname, options)
      TableScanner(q, distance, k, tablename, tidList)
    }
  }


  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param tablename
   */
  def progressiveQuery(q: WorkingVector, distance : NormBasedDistanceFunction, k : Int, tablename: TableName, onComplete : (Seq[Result]) => Unit): Int = {
    val indexes = Index.getIndexnames(tablename)

    val options = mMap[String, String]()
    options += "k" -> k.toString
    options += "norm" -> distance.n.toString

    //TODO: // here we should actually keep the index information, especially on the level of onComplete: if the index returns exact information, we should stop the
    //execution completely; if the index returns only approximate information we should keep the retrieving process running
    indexes
      .map(indexname => indexQueryNonBlocking(q, distance, k, indexname, options.toMap))
      .map(action => action.onComplete(x => onComplete(x.get)))
    sequentialQueryNonBlocking(q, distance, k, tablename).onComplete(x => onComplete(x.get))

    indexes.length + 1
  }
}