package org.vitrivr.adampro.query.execution.parallel

import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.data.entity.Entity.EntityName
import org.vitrivr.adampro.utils.exception.GeneralAdamException
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.query.ast.generic.{QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.ast.internal.BooleanFilterExpression.BooleanFilterScanExpression
import org.vitrivr.adampro.query.execution._
import org.vitrivr.adampro.query.query.{FilteringQuery, RankingQuery}
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
    * @param onNext  function to execute on next
    * @param options     options applied when evaluating query
    * @param id          query id
    * @return
    */
  def parallelQuery[U](entityname: EntityName, nnq: RankingQuery, bq: Option[FilteringQuery], pathChooser: ParallelPathChooser, onNext: Try[ProgressiveObservation] => U, options: Option[QueryEvaluationOptions], id: Option[String])(tracker : QueryTracker)(implicit ac: SharedComponentContext): ParallelQueryStatusTracker = {
    val filter = if (bq.isDefined) {
      new BooleanFilterScanExpression(entityname)(bq.get, None)(None)(ac).rewrite().execute(options)(tracker)
    } else {
      None
    }

    val paths = pathChooser.getPaths(entityname, nnq)
    val distinctPaths = paths.distinct

    if (paths.length != distinctPaths.length) {
      log.debug("removed " + (distinctPaths.length - paths.length) + " paths for parallel querying, which were duplicates")
    }

    parallelQuery(distinctPaths, filter, onNext, options, id)(tracker)
  }


  /**
    * Performs a parallel query, i.e., all indexes and sequential search are started at the same time and results are returned as soon
    * as they are available. When a precise result is returned, the whole query is stopped.
    *
    * @param exprs      query expressions to execute
    * @param onNext function to execute on complete
    * @param options    options applied when evaluating query
    * @param id         query id
    * @return a tracker for the parallel query
    */
  private def parallelQuery[U](exprs: Seq[QueryExpression], filter: Option[DataFrame], onNext: Try[ProgressiveObservation] => U, options: Option[QueryEvaluationOptions], id: Option[String])(tracker : QueryTracker)(implicit ac: SharedComponentContext): ParallelQueryStatusTracker = {
    val pqtracker = new ParallelQueryStatusTracker(id.getOrElse(""))
    log.debug("performing parallel query with " + exprs.length + " paths: " + exprs.map(expr => expr.info.scantype.getOrElse("<missing scantype>") + " (" + expr.info.source.getOrElse("<missing source>") + ")").mkString(", "))

    if (exprs.isEmpty) {
      throw new GeneralAdamException("no paths for parallel query set; possible causes: is the entity or the attribute existing?")
    }

    val scanFutures = exprs.map(expr => new ScanFuture(expr, filter, onNext, pqtracker)(tracker))
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
  def timedParallelQuery[U](entityname: EntityName, nnq: RankingQuery, bq: Option[FilteringQuery], pathChooser: ParallelPathChooser, timelimit: Duration, options: Option[QueryEvaluationOptions], id: Option[String])(tracker : QueryTracker)(implicit ac: SharedComponentContext): ProgressiveObservation = {
    val filter = if (bq.isDefined) {
      new BooleanFilterScanExpression(entityname)(bq.get, None)(None)(ac).rewrite().execute(options)(tracker)
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
  def timedParallelQuery(exprs: Seq[QueryExpression], timelimit: Duration, filter: Option[DataFrame], options: Option[QueryEvaluationOptions], id: Option[String])(tracker : QueryTracker)(implicit ac: SharedComponentContext): ProgressiveObservation = {
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
