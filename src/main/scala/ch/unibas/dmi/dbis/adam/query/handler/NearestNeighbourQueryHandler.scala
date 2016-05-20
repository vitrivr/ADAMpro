package ch.unibas.dmi.dbis.adam.query.handler

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.QueryNotConformException
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.index.{Index}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.{ProgressiveQueryIntermediateResults, ProgressiveQueryStatus, ProgressiveQueryStatusTracker}
import ch.unibas.dmi.dbis.adam.query.progressive._
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

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
private[query] object NearestNeighbourQueryHandler extends Logging {
  private def isQueryConform(entityname: EntityName, query: NearestNeighbourQuery)(implicit ac: AdamContext): Boolean = {
    val entity = Entity.load(entityname).get
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
  def sequential(entityname: EntityName, query: NearestNeighbourQuery, filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
    log.debug("performing sequential nearest neighbor scan")
    if (!isQueryConform(entityname, query)) {
      throw QueryNotConformException()
    }

    FeatureScanner(Entity.load(entityname).get, query, filter)
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
  def indexQuery(indexname: IndexName, query: NearestNeighbourQuery, filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
    indexQuery(Index.load(indexname).get, query, filter)
  }

  /**
    * Performs an index-based query.
    *
    * @param index
    * @param query
    * @param filter
    * @return depending on whether query.indexOnly is set to true only the index tuples are scanned,
    *         otherwise also the true data is scanned for performing the query
    */
  def indexQuery(index: Index, query: NearestNeighbourQuery, filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
    log.debug("performing index-based nearest neighbor scan")

    val entityname = index.entityname
    if (!isQueryConform(entityname, query)) {
      throw QueryNotConformException()
    }

    var res = index.scan(query, filter)

    if (!query.indexOnly) {
      log.debug("starting index scanner")
      val entity = Entity.load(entityname).get

      res = FeatureScanner(entity, query, Some(res))
    }

    res
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
  def progressiveQuery[U](entityname: EntityName, query: NearestNeighbourQuery, filter: Option[DataFrame], paths: ProgressivePathChooser, onComplete: (ProgressiveQueryStatus.Value, DataFrame, Float, String, Map[String, String]) => U)(implicit ac: AdamContext): ProgressiveQueryStatusTracker = {
    log.debug("starting progressive query scanner")
    if (!isQueryConform(entityname, query)) {
      throw QueryNotConformException()
    }

    val options = mMap[String, String]()

    val tracker = new ProgressiveQueryStatusTracker(query.queryID.get)


    val indexScanFutures = paths.getPaths(entityname).map { indexname =>
      //TODO: possibly switch between index only and full result scan (give user the option to choose!)
      val isf = new IndexScanFuture(indexname, query, onComplete, tracker)
    }

    //TODO: possibly re-add sequential scan
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
  def timedProgressiveQuery(entityname: EntityName, query: NearestNeighbourQuery, filter: Option[DataFrame], paths: ProgressivePathChooser, timelimit: Duration)(implicit ac: AdamContext): ProgressiveQueryIntermediateResults = {
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


