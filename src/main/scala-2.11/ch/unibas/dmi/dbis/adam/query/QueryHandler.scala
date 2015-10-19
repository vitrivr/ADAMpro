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
import ch.unibas.dmi.dbis.adam.table.Tuple.TupleID
import org.apache.spark.Logging

import scala.collection.immutable.HashSet
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
  private val metadataStorage = SparkStartup.metadataStorage
  private val tableStorage = SparkStartup.tableStorage

  /**
   *
   * @param where
   * @param tablename
   * @return
   */
  def metadataQuery(where : Map[String, String], tablename: TableName): HashSet[TupleID] ={
    val filter = where.map(c => c._1 + " = " + c._2).mkString(" AND ")
    val res = metadataStorage.readTable(tablename).getData.filter(filter).map(r => r.getLong(0)).collect()

    HashSet(res : _*)
  }


  /**
   *
   * @param where
   * @param tablename
   * @return
   */
  def metadataQuery(where : String, tablename : TableName) : HashSet[TupleID] ={
    val res = metadataStorage.readTable(tablename).getData.filter(where).map(r => r.getLong(0)).collect()

    HashSet(res : _*)
  }



  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param tablename
   * @return
   */
  def sequentialQuery(q: WorkingVector, distance : DistanceFunction, k : Int, tablename: TableName, preselection : HashSet[TupleID] = null): Seq[Result] = {
    TableScanner(q, distance, k, tablename, preselection)
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
  def indexQuery(q: WorkingVector, distance : DistanceFunction, k : Int, indexname : IndexName, options : Map[String, String], preselection : HashSet[TupleID] = null): Seq[Result] = {
    val onlyIndexResults = options.getOrElse("onlyindex", "false").toBoolean

    if(!onlyIndexResults){
      indexAndTableScan(q, distance, k, indexname, options, preselection)
    } else {
      indexScanOnly(q, distance, k, indexname, options, preselection)
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
  def indexAndTableScan(q: WorkingVector, distance : DistanceFunction, k : Int, indexname : IndexName, options : Map[String, String], preselection : HashSet[TupleID] = null): Seq[Result] = {
    val tablename = CatalogOperator.getIndexTableName(indexname)

    val tableFuture = Future {
      Table.retrieveTable(tablename)
    }

    val tidList = IndexScanner(q, distance, k, indexname, options, preselection)

    val table = Await.result[Table](tableFuture, Duration(100, TimeUnit.SECONDS))
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
  def indexScanOnly(q: WorkingVector, distance : DistanceFunction, k : Int, indexname : IndexName, options : Map[String, String], preselection : HashSet[TupleID] = null): Seq[Result] = {
    IndexScanner(q, distance, k, indexname, options, preselection).toList.map(tid => Result(-1, tid))
  }

  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param tablename
   */
  def progressiveQuery(q: WorkingVector, distance : NormBasedDistanceFunction, k : Int, tablename: TableName, onComplete : (ProgressiveQueryStatus, Seq[Result], Map[String, String]) => Unit, preselection : HashSet[TupleID] = null): Int = {
    val indexes: Seq[IndexName] = Index.getIndexnames(tablename)

    val options = mMap[String, String]()
    options += "k" -> k.toString
    options += "norm" -> distance.n.toString

    val status = new ProgressiveQueryStatus()
    status.startAll(indexes)
    status.start(tablename)

    val qid = java.util.UUID.randomUUID.toString

    //TODO: // here we should actually keep the index information, especially on the level of onComplete: if the index returns exact information, we should stop the
    //execution completely; if the index returns only approximate information we should keep the retrieving process running
    indexes
      .map{indexname =>
        val indextypename = Index.retrieveIndex(indexname).indextypename
        val info =  Map[String,String]("type" -> ("index: " + indextypename), "relation" -> tablename, "index" -> indexname, "qid" -> qid)
        Future {indexQuery(q, distance, k, indexname, options.toMap)}.onComplete(x => {status.end(indexname); onComplete(status, x.get, info)})
    }

    val info =  Map[String,String]("type" -> "sequential", "relation" -> tablename, "qid" -> qid)
    Future{sequentialQuery(q, distance, k, tablename)}.onComplete(x => {status.end(tablename); onComplete(status, x.get, info)})

    indexes.length + 1
  }
}