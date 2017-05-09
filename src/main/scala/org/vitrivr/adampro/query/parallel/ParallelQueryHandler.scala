package org.vitrivr.adampro.query.parallel

import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.handler.generic.{QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.handler.internal.BooleanFilterExpression.BooleanFilterScanExpression
import org.vitrivr.adampro.query.progressive._
import org.vitrivr.adampro.query.query.{BooleanQuery, NearestNeighbourQuery}
import org.vitrivr.adampro.utils.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

/**
  * adamtwo
  *
  * Ivan Giangreco
  * November 2015
  */
object ParallelQueryHandler extends Logging {
  /**
    *
    * @param entityname  name of entity
    * @param nnq         information for nearest neighbour query
    * @param bq          information for boolean query
    * @param pathChooser parallel query path chooser
    * @param onComplete  function to execute on complete
    * @param options     options applied when evaluating query
    * @param id          query id
    * @return
    */
  def parallelQuery[U](entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], pathChooser: ParallelPathChooser, onComplete: Try[ProgressiveObservation] => U, options: Option[QueryEvaluationOptions], id: Option[String])(tracker : OperationTracker)(implicit ac: AdamContext): ParallelQueryStatusTracker = {
    val filter = if (bq.isDefined) {
      new BooleanFilterScanExpression(entityname)(bq.get, None)(None)(ac).prepareTree().evaluate(options)(tracker)
    } else {
      None
    }

    val paths = pathChooser.getPaths(entityname, nnq)
    val distinctPaths = paths.distinct

    if (paths.length != distinctPaths.length) {
      log.debug("removed " + (distinctPaths.length - paths.length) + " paths for parallel querying, which were duplicates")
    }

    parallelQuery(distinctPaths, filter, onComplete, options, id)(tracker)
  }


  /**
    * Performs a parallel query, i.e., all indexes and sequential search are started at the same time and results are returned as soon
    * as they are available. When a precise result is returned, the whole query is stopped.
    *
    * @param exprs      query expressions to execute
    * @param onComplete function to execute on complete
    * @param options    options applied when evaluating query
    * @param id         query id
    * @return a tracker for the parallel query
    */
  private def parallelQuery[U](exprs: Seq[QueryExpression], filter: Option[DataFrame], onComplete: Try[ProgressiveObservation] => U, options: Option[QueryEvaluationOptions], id: Option[String])(tracker : OperationTracker)(implicit ac: AdamContext): ParallelQueryStatusTracker = {
    val pqtracker = new ParallelQueryStatusTracker(id.getOrElse(""))
    log.debug("performing parallel query with " + exprs.length + " paths: " + exprs.map(expr => expr.info.scantype.getOrElse("<missing scantype>") + " (" + expr.info.source.getOrElse("<missing source>") + ")").mkString(", "))

    if (exprs.isEmpty) {
      throw new GeneralAdamException("no paths for parallel query set; possible causes: is the entity or the attribute existing?")
    }

    val scanFutures = exprs.map(expr => new ScanFuture(expr, filter, onComplete, pqtracker)(tracker))
    pqtracker
  }


  /**
    *
    * @param entityname  name of entity
    * @param nnq         information for nearest neighbour query
    * @param bq          information for boolean query
    * @param pathChooser parallel query path chooser
    * @param timelimit   maximum time to wait for results
    * @param options     options applied when evaluating query
    * @param id          query id
    * @return
    */
  def timedParallelQuery[U](entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], pathChooser: ParallelPathChooser, timelimit: Duration, options: Option[QueryEvaluationOptions], id: Option[String])(tracker : OperationTracker)(implicit ac: AdamContext): ProgressiveObservation = {
    val filter = if (bq.isDefined) {
      new BooleanFilterScanExpression(entityname)(bq.get, None)(None)(ac).prepareTree().evaluate(options)(tracker)
    } else {
      None
    }

    val paths = pathChooser.getPaths(entityname, nnq).distinct
    val distinctPaths = paths.distinct

    if (paths.length != distinctPaths.length) {
      log.debug("removed " + (distinctPaths.length - paths.length) + " paths for parallel querying, which were duplicates")
    }

    timedParallelQuery(distinctPaths, timelimit, filter, options, id)(tracker)
  }


  /**
    * Performs a timed parallel query, i.e., it performs the query for a maximum of the given time limit and returns then the best possible
    * available results.
    *
    * @param exprs     query expressions to execute
    * @param timelimit maximum time to wait  for results
    * @param options   options applied when evaluating query
    * @param id        query id
    * @return the results available together with a confidence score
    */
  def timedParallelQuery(exprs: Seq[QueryExpression], timelimit: Duration, filter: Option[DataFrame], options: Option[QueryEvaluationOptions], id: Option[String])(tracker : OperationTracker)(implicit ac: AdamContext): ProgressiveObservation = {
    log.debug("timed parallel query performs kNN query")
    val pqtracker = parallelQuery[Unit](exprs, filter, (observation: Try[ProgressiveObservation]) => (), options, id)(tracker)

    val timerFuture = Future {
      val sleepTime = Duration(500.toLong, "millis")
      var nSleep = (timelimit / sleepTime).toInt

      while (pqtracker.status != ProgressiveQueryStatus.FINISHED && nSleep > 0) {
        nSleep -= 1
        Thread.sleep(sleepTime.toMillis)
      }
    }

    Await.result(timerFuture, timelimit)
    pqtracker.stop()

    pqtracker.results.observation
  }
}
