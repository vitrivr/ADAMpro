package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, NormBasedDistanceFunction}
import ch.unibas.dmi.dbis.adam.storage.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import org.apache.spark.FutureAction

import scala.collection.mutable.{Map => mMap}
import scala.concurrent.ExecutionContext.Implicits.global


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
  def sequentialQuery(q: WorkingVector, distance : DistanceFunction, k : Int, tablename: TableName): FutureAction[Seq[Result]] = {
    SequentialScanner(q, distance, k, tablename)
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
  def indexQuery(q: WorkingVector, distance : DistanceFunction, k : Int, indexname : IndexName, options : Map[String, String]): FutureAction[Seq[Result]] = {
    val tablename = CatalogOperator.getIndexTableName(indexname)
    val tidList = IndexScanner(q, distance, k, indexname, options)
    SequentialScanner(q, distance, k, tablename, tidList.value.get.get)
  }


  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param tablename
   */
  def progressiveQuery(q: WorkingVector, distance : NormBasedDistanceFunction, k : Int, tablename: TableName, onComplete : (Seq[Result]) => Unit): Unit ={
    val indexes = Index.getIndexnames(tablename)

    val options = mMap[String, String]()
    options += "k" -> k.toString
    options += "norm" -> distance.n.toString

    //TODO: // here we should actually keep the index information, especially on the level of onComplete: if the index returns exact information, we should stop the
    //execution completely; if the index returns only approximate information we should keep the retrieving process running
    indexes
      .map(indexname => indexQuery(q, distance, k, indexname, options.toMap))
      .map(action => action.onComplete(x => onComplete(x.get)))

    sequentialQuery(q, distance, k, tablename).onComplete(x => onComplete(x.get))
  }
}