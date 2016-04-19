package ch.unibas.dmi.dbis.adam.query.handler

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.exception.QueryNotConformException
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.index.IndexHandler
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.datastructures.{ProgressiveQueryStatus, ProgressiveQueryStatusTracker, ProgressiveQueryIntermediateResults}
import ch.unibas.dmi.dbis.adam.query.progressive._
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.query.scanner.{FeatureScanner, IndexScanner}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row}

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
private[query] object NearestNeighbourQueryHandler {
  val log = Logger.getLogger(getClass.getName)

  private def isQueryConform(entityname: EntityName, query: NearestNeighbourQuery)(implicit ac: AdamContext): Boolean = {
    val entity = EntityHandler.load(entityname).get
    entity.isQueryConform(query)
  }

  /**
    * Performs a sequential query, i.e., without using any index structure.
    *
    * @param entityname
    * @param query
    * @param filter
    * @return
    */
  def sequential(entityname: EntityName, query: NearestNeighbourQuery, filter: Option[Set[TupleID]])(implicit ac: AdamContext): DataFrame = {
    log.debug("performing sequential nearest neighbor scan")
    if (!isQueryConform(entityname, query)) {
      throw QueryNotConformException()
    }

    FeatureScanner(EntityHandler.load(entityname).get, query, filter)
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
  def indexQuery(indexname: IndexName, query: NearestNeighbourQuery, filter: Option[Set[TupleID]])(implicit ac: AdamContext): DataFrame = {
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
  def indexQueryWithResults(indexname: IndexName, query: NearestNeighbourQuery, filter: Option[Set[TupleID]])(implicit ac: AdamContext): DataFrame = {
    val entityname = CatalogOperator.getEntitynameFromIndex(indexname)
    if (!isQueryConform(entityname, query)) {
      throw QueryNotConformException()
    }

    log.debug("starting index scanner")
    val tidList = IndexScanner(IndexHandler.load(indexname).get, query, filter)

    val entity = EntityHandler.load(entityname).get

    log.debug("starting feature scanner")
    FeatureScanner(entity, query, Some(tidList.map(_.tid)))
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
  def indexOnlyQuery(indexname: IndexName, query: NearestNeighbourQuery, filter: Option[Set[TupleID]])(implicit ac: AdamContext): DataFrame = {
    val entityname = CatalogOperator.getEntitynameFromIndex(indexname)
    if (!isQueryConform(entityname, query)) {
      throw QueryNotConformException()
    }

    log.debug("starting index scanner")
    val result = IndexScanner(IndexHandler.load(indexname).get, query, filter).toSeq
    val rdd = ac.sc.parallelize(result).map(res => Row(res.distance, res.tid))
    ac.sqlContext.createDataFrame(rdd, Result.resultSchema)
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
  def progressiveQuery[U](entityname: EntityName, query: NearestNeighbourQuery, filter: Option[Set[TupleID]], paths: ProgressivePathChooser, onComplete: (ProgressiveQueryStatus.Value, DataFrame, Float, String, Map[String, String]) => U)(implicit ac: AdamContext): ProgressiveQueryStatusTracker = {
    log.debug("starting progressive query scanner")
    if (!isQueryConform(entityname, query)) {
      throw QueryNotConformException()
    }

    val options = mMap[String, String]()

    val tracker = new ProgressiveQueryStatusTracker(query.queryID.get)


    val indexScanFutures = paths.getPaths[U](entityname).par.map { indexname =>
      val isf = new IndexScanFuture(indexname, query, onComplete, tracker)
    }

    //sequential
    //val ssf = new SequentialScanFuture(entityname, query, onComplete, tracker)

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
  def timedProgressiveQuery(entityname: EntityName, query: NearestNeighbourQuery, filter: Option[Set[TupleID]], paths: ProgressivePathChooser, timelimit: Duration)(implicit ac: AdamContext): ProgressiveQueryIntermediateResults = {
    log.debug("starting timed progressive query scanner")

    val tracker = progressiveQuery[Unit](entityname, query, filter, paths, (status, result, confidence, source, info) => ())

    val timerFuture = Future {
      val sleepTime = Duration(500.toLong, "millis")
      var nSleep = (timelimit / sleepTime).toInt

      while (tracker.status != ProgressiveQueryStatus.FINISHED && nSleep > 0) {
        nSleep -= 1
        Thread.sleep(sleepTime.toMillis)
      }
    }

    Await.result(timerFuture, timelimit)
    tracker.stop()
    log.debug("timed progressive query stopped")
    tracker.results
  }
}


