package org.vitrivr.adampro.query.progressive

import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.handler.generic.{QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.utils.Logging
import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.helpers.tracker.OperationTracker

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
class ScanFuture[U](expression: QueryExpression, filter : Option[DataFrame], onComplete: Try[ProgressiveObservation] => U, val pqtracker: ProgressiveQueryStatusTracker, options: Option[QueryEvaluationOptions] = None)(tracker : OperationTracker)(implicit ac: AdamContext) extends Logging{
  pqtracker.register(this)

  val t1 = System.currentTimeMillis()

  val future = Future {
    expression.prepareTree().evaluate(options)(tracker)
  }
  future.onSuccess({
    case res =>
      pqtracker.synchronized {

        val information = expression.information()
        val typename = information.source
        val info = information.info

        val observation = ProgressiveObservation(pqtracker.status, res, confidence.getOrElse(0), typename.getOrElse(""), info, t1, System.currentTimeMillis())
        if (!pqtracker.isCompleted) {
          onComplete(Success(observation))
        }

        log.debug("completed scanning of " + typename.getOrElse("<missing information>"))

        pqtracker.notifyCompletion(this, observation)
      }
  })
  future.onFailure({
    case res =>
      if (!pqtracker.isCompleted) {
        onComplete(Failure(res))
      }

      log.error("error when running progressive query", res)
  })

  val confidence: Option[Float] = expression.info.confidence
}