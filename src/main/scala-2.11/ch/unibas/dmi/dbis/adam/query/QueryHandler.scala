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
  def metadataQuery(where: Map[String, String], tablename: TableName): HashSet[TupleID] = {
    val filter = where.map(c => c._1 + " = " + c._2).mkString(" AND ")
    val res = metadataStorage.readTable(tablename).getData.filter(filter).map(r => r.getLong(0)).collect()

    HashSet(res: _*)
  }


  /**
   *
   * @param where
   * @param tablename
   * @return
   */
  def metadataQuery(where: String, tablename: TableName): HashSet[TupleID] = {
    val res = metadataStorage.readTable(tablename).getData.filter(where).map(r => r.getLong(0)).collect()

    HashSet(res: _*)
  }


  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param tablename
   * @return
   */
  def sequentialQuery(q: WorkingVector, distance: DistanceFunction, k: Int, tablename: TableName, filter: Option[HashSet[TupleID]], queryID : String = ""): Seq[Result] = {
    TableScanner(Table.retrieveTable(tablename), q, distance, k, filter, queryID)
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
  def indexQuery(q: WorkingVector, distance: DistanceFunction, k: Int, indexname: IndexName, options: Map[String, String], filter: Option[HashSet[TupleID]], queryID : String = ""): Seq[Result] = {
    val onlyIndexResults = options.getOrElse("onlyindex", "false").toBoolean

    if (!onlyIndexResults) {
      indexAndTableScan(q, distance, k, indexname, options, filter, queryID)
    } else {
      indexScanOnly(q, distance, k, indexname, options, filter, queryID)
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
  def indexAndTableScan(q: WorkingVector, distance: DistanceFunction, k: Int, indexname: IndexName, options: Map[String, String], filter: Option[HashSet[TupleID]], queryID : String = ""): Seq[Result] = {
    val tablename = CatalogOperator.getIndexTableName(indexname)

    val tableFuture = Future {
      Table.retrieveTable(tablename)
    }

    val tidList = IndexScanner(q, distance, k, indexname, options, filter, queryID)

    val table = Await.result[Table](tableFuture, Duration(100, TimeUnit.SECONDS))
    TableScanner(table, q, distance, k, Some(tidList), queryID)
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
  def indexScanOnly(q: WorkingVector, distance: DistanceFunction, k: Int, indexname: IndexName, options: Map[String, String], filter: Option[HashSet[TupleID]], queryID : String = ""): Seq[Result] = {
    IndexScanner(q, distance, k, indexname, options, filter).toList.map(tid => Result(-1, tid))
  }

  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param tablename
   */
  def progressiveQuery(q: WorkingVector, distance: NormBasedDistanceFunction, k: Int, tablename: TableName, filter: Option[HashSet[TupleID]], onComplete: (ProgressiveQueryStatus.Value, Seq[Result], Map[String, String]) => Unit, queryID : String = ""): Int = {
    val indexnames = Index.getIndexnames(tablename)

    val options = mMap[String, String]()
    options += "k" -> k.toString
    options += "norm" -> distance.n.toString

    val tracker = new ProgressiveQueryStatusTracker()

    //index scans
    val indexScanFutures = indexnames.par.map { indexname =>
      val isf = new IndexScanFuture(indexname, q, distance, k, options.toMap, onComplete, queryID, tracker)
    }

    //sequential scan
    val ssf = new SequentialScanFuture(tablename, q, distance, k, onComplete, queryID, tracker)


    //number of queries running (indexes + sequential)
    indexnames.length + 1
  }
}


