package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures._
import ch.unibas.dmi.dbis.adam.query.handler.generic.QueryExpression
import ch.unibas.dmi.dbis.adam.query.handler.internal.BooleanFilterExpression.BooleanFilterScanExpression
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * November 2015
  */
object ProgressiveQueryHandler extends Logging {
  /**
    *
    * @param entityname
    * @param nnq
    * @param bq
    * @param pathChooser
    * @param onComplete
    * @param id
    * @return
    */
  def progressiveQuery[U](entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], pathChooser: ProgressivePathChooser, onComplete: ProgressiveObservation => U, id: Option[String] = None)(implicit ac: AdamContext): ProgressiveQueryStatusTracker = {
    val filter = if (bq.isDefined) {
      new BooleanFilterScanExpression(entityname)(bq.get)().prepareTree().evaluate()
    } else {
      None
    }
    progressiveQuery(pathChooser.getPaths(entityname, nnq), filter, onComplete, id)
  }


  /**
    * Performs a progressive query, i.e., all indexes and sequential search are started at the same time and results are returned as soon
    * as they are available. When a precise result is returned, the whole query is stopped.
    *
    * @param exprs query expressions to execute
    * @param onComplete
    * @param id
    * @return a tracker for the progressive query
    */
  def progressiveQuery[U](exprs: Seq[QueryExpression], filter: Option[DataFrame], onComplete: ProgressiveObservation => U, id: Option[String] = None)(implicit ac: AdamContext): ProgressiveQueryStatusTracker = {
    val tracker = new ProgressiveQueryStatusTracker(id.getOrElse(""))
    log.debug("performing progressive query with " + exprs.length + " paths: " + exprs.map(expr => expr.info.scantype.getOrElse("<missing scantype>") + " (" + expr.info.source.getOrElse("<missing source>") + ")").mkString(", "))

    if(exprs.isEmpty){
      //TODO: check that errors are sent back to client
      throw new GeneralAdamException("no paths for progressive query set; possible causes: is the entity or the attribute existing?")
    }

    val scanFutures = exprs.map(expr => new ScanFuture(expr, filter, onComplete, tracker))
    tracker
  }


  /**
    *
    * @param entityname
    * @param nnq
    * @param bq
    * @param pathChooser
    * @param timelimit
    * @param id
    * @return
    */
  def timedProgressiveQuery[U](entityname: EntityName, nnq: NearestNeighbourQuery, bq: Option[BooleanQuery], pathChooser: ProgressivePathChooser, timelimit: Duration, id: Option[String] = None)(implicit ac: AdamContext): ProgressiveObservation = {
    val filter = if (bq.isDefined) {
      new BooleanFilterScanExpression(entityname)(bq.get)().prepareTree().evaluate()
    } else {
      None
    }
    timedProgressiveQuery(pathChooser.getPaths(entityname, nnq), timelimit, filter, id)
  }


  /**
    * Performs a timed progressive query, i.e., it performs the query for a maximum of the given time limit and returns then the best possible
    * available results.
    *
    * @param exprs     query expressions to execute
    * @param timelimit maximum time to wait
    * @param id
    * @return the results available together with a confidence score
    */
  def timedProgressiveQuery(exprs: Seq[QueryExpression], timelimit: Duration, filter: Option[DataFrame], id: Option[String] = None)(implicit ac: AdamContext): ProgressiveObservation = {
    log.debug("timed progressive query performs kNN query")
    val tracker = progressiveQuery[Unit](exprs, filter, (observation: ProgressiveObservation) => ())

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

    tracker.results.observation
  }
}
