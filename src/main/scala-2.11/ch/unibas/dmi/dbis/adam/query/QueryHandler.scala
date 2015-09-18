package ch.unibas.dmi.dbis.adam.query

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, NormBasedDistanceFunction}
import ch.unibas.dmi.dbis.adam.storage.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import org.apache.spark.Logging

import scala.collection.mutable.{Map => mMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object QueryHandler extends Logging {
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
   * @param indexname
   * @param options
   * @return
   */
  def indexQuery(q: WorkingVector, distance : DistanceFunction, k : Int, indexname : IndexName, options : Map[String, String]): Seq[Result] = {
    val onlyIndexResults = options.getOrElse("onlyindex", "false").toBoolean

    if(!onlyIndexResults){
      indexAndTableScan(q, distance, k, indexname, options)
    } else {
      indexScanOnly(q, distance, k, indexname, options)
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
  def indexAndTableScan(q: WorkingVector, distance : DistanceFunction, k : Int, indexname : IndexName, options : Map[String, String]): Seq[Result] = {
    val tablename = CatalogOperator.getIndexTableName(indexname)

    val tableFuture = Future {
      Table.retrieveTable(tablename)
    }

    val tidList = IndexScanner(q, distance, k, indexname, options)

    val table = Await.result[Table](tableFuture, Duration(30, TimeUnit.SECONDS))
    TableScanner(table, q, distance, k, tidList)
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
  def indexScanOnly(q: WorkingVector, distance : DistanceFunction, k : Int, indexname : IndexName, options : Map[String, String]): Seq[Result] = {
    IndexScanner(q, distance, k, indexname, options).map(tid => Result(-1, tid))
  }

  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param tablename
   */
  def progressiveQuery(q: WorkingVector, distance : NormBasedDistanceFunction, k : Int, tablename: TableName, onComplete : (Seq[Result], Map[String, String]) => Unit): Int = {
    val indexes = Index.getIndexnames(tablename)

    val options = mMap[String, String]()
    options += "k" -> k.toString
    options += "norm" -> distance.n.toString


    //TODO: // here we should actually keep the index information, especially on the level of onComplete: if the index returns exact information, we should stop the
    //execution completely; if the index returns only approximate information we should keep the retrieving process running
    indexes
      .map{indexname =>
        val info =  Map[String,String]("type" -> "index", "relation" -> tablename, "index" -> indexname)
        Future {indexQuery(q, distance, k, indexname, options.toMap)}.onComplete(x => onComplete(x.get, info))
    }

    val info =  Map[String,String]("type" -> "sequential", "relation" -> tablename)
    Future{sequentialQuery(q, distance, k, tablename)}.onComplete(x => onComplete(x.get, info))

    indexes.length + 1
  }
}