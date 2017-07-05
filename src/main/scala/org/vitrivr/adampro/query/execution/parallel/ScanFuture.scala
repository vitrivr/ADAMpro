package org.vitrivr.adampro.query.execution.parallel

import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.query.ast.generic.{QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.execution.ProgressiveObservation
import org.vitrivr.adampro.utils.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
class ScanFuture[U](expression: QueryExpression, filter : Option[DataFrame], onComplete: Try[ProgressiveObservation] => U, val pqtracker: ParallelQueryStatusTracker, options: Option[QueryEvaluationOptions] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext) extends Logging{
  pqtracker.register(this)

  val t1 = System.currentTimeMillis()

  val future = Future {
    expression.prepareTree().evaluate(options)(tracker)
  }
  future.onSuccess({
    case res =>
      pqtracker.synchronized {

        val info = expression.information()
        val typename = info.source

        val observation = ProgressiveObservation(pqtracker.status, res, confidence.getOrElse(0), typename.getOrElse(""), info.toMap, t1, System.currentTimeMillis())
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

      log.error("error when running parallel query", res)
  })

  val confidence: Option[Float] = expression.info.confidence
}