package ch.unibas.dmi.dbis.adam.query.handler

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.progressive._
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.query.scanner.{FeatureScanner, IndexScanner}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row}

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
object NearestNeighbourQueryHandler {
  val log = Logger.getLogger(getClass.getName)

  /**
    * Performs a sequential query, i.e., without using any index structure.
    *
    * @param entityname
    * @param query
    * @param filter
    * @return
    */
  def sequential(entityname: EntityName, query: NearestNeighbourQuery, filter: Option[HashSet[TupleID]]): DataFrame = {
    log.debug("performing sequential nearest neighbor scan")
    FeatureScanner(Entity.load(entityname), query, filter)
  }

  /**
    * Performs an index-based query.
    *
    * @param indexname
    * @param query
    * @param filter
    * @return depending on whether query.indexOnly is set to true only the index tuples are scanned,
    *         otherwise also the true data is scanned for performing the query
    */
  def indexQuery(indexname: IndexName, query: NearestNeighbourQuery, filter: Option[HashSet[TupleID]]): DataFrame = {
    log.debug("performing index-based nearest neighbor scan")

    if (query.indexOnly) {
      log.debug("reading only index")
      indexOnlyQuery(indexname, query, filter)
    } else {
      log.debug("reading index with results")
      indexQueryWithResults(indexname, query, filter)
    }
  }

  /**
    * Performs an index-based query. Similar to traditional DBMS, a query is performed is on the index and then the true
    * data is accessed.
    *
    * @param indexname
    * @param query
    * @param filter
    * @return
    */
  def indexQueryWithResults(indexname: IndexName, query: NearestNeighbourQuery, filter: Option[HashSet[TupleID]]): DataFrame = {
    val entityname = CatalogOperator.getEntitynameFromIndex(indexname)

    val future = Future {
      Entity.load(entityname)
    }

    log.debug("starting index scanner")
    val tidList = IndexScanner(Index.load(indexname), query, filter)

    val entity = Await.result[Entity](future, Duration(100, TimeUnit.SECONDS))

    log.debug("starting feature scanner")
    FeatureScanner(entity, query, Option(tidList))
  }

  /**
    * Performs an index-based query. Unlike in traditional databases, this query returns result candidates, but may contain
    * false positives as well.
    *
    * @param indexname
    * @param query
    * @param filter
    * @return
    */
  def indexOnlyQuery(indexname: IndexName, query: NearestNeighbourQuery, filter: Option[HashSet[TupleID]]): DataFrame = {
    log.debug("starting index scanner")
    val result = IndexScanner(Index.load(indexname), query, filter).toSeq
    val rdd = SparkStartup.sc.parallelize(result).map(res => Row(0, res))
    SparkStartup.sqlContext.createDataFrame(rdd, Result.resultSchema)
  }

  /**
    * Performs a progressive query, i.e., all indexes and sequential search are started at the same time and results are returned as soon
    * as they are available. When a precise result is returned, the whole query is stopped.
    *
    * @param entityname
    * @param query
    * @param filter
    * @param onComplete
    * @return
    */
  def progressiveQuery(entityname: EntityName, query: NearestNeighbourQuery, filter: Option[HashSet[TupleID]], onComplete: (ProgressiveQueryStatus.Value, DataFrame, Float, Map[String, String]) => Unit): ProgressiveQueryStatusTracker = {
    log.debug("starting progressive query scanner")

    val indexnames = Index.list(entityname)

    val options = mMap[String, String]()

    val tracker = new ProgressiveQueryStatusTracker(query.queryID.get)

    //index scans
    val indexScanFutures = indexnames.par.map { indexname =>
      val isf = new IndexScanFuture(indexname, query, onComplete, tracker)
    }

    //sequential scan
    val ssf = new SequentialScanFuture(entityname, query, onComplete, tracker)

    tracker
  }


  /**
    * Performs a timed progressive query, i.e., it performs the query for a maximum of the given time limit and returns then the best possible
    * available results.
    *
    * @param entityname
    * @param query
    * @param filter
    * @return
    */
  def timedProgressiveQuery(entityname: EntityName, query: NearestNeighbourQuery, filter: Option[HashSet[TupleID]], timelimit: Duration): (DataFrame, Float) = {
    log.debug("starting timed progressive query scanner")

    val indexnames = Index.list(entityname)

    val options = mMap[String, String]()

    val tracker = new ProgressiveQueryStatusTracker(query.queryID.get)

    val timerFuture = Future {
      val sleepTime = Duration(500.toLong, "millis")
      var nSleep = (timelimit / sleepTime).toInt

      while (tracker.status != ProgressiveQueryStatus.FINISHED && nSleep > 0) {
        nSleep -= 1
        Thread.sleep(sleepTime.toMillis)
      }
    }

    //index scans
    val indexScanFutures = indexnames.par.map { indexname =>
      val isf = new IndexScanFuture(indexname, query, (status, result, confidence, info) => (), tracker)
    }

    //sequential scan
    val ssf = new SequentialScanFuture(entityname, query, (status, result, confidence, info) => (), tracker)

    Await.result(timerFuture, timelimit)
    tracker.stop()
    log.debug("timed progressive query stopped")
    tracker.results
  }
}


