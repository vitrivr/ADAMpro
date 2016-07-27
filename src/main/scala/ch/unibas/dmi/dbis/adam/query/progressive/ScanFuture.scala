package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{QueryEvaluationOptions, QueryExpression}
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.DataFrame

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
class ScanFuture[U](expression: QueryExpression, filter : Option[DataFrame], onComplete: Try[ProgressiveObservation] => U, val tracker: ProgressiveQueryStatusTracker, options: Option[QueryEvaluationOptions] = None)(implicit ac: AdamContext) extends Logging{
  tracker.register(this)

  val t1 = System.currentTimeMillis()

  val future = Future {
    expression.prepareTree().evaluate(options)
  }
  future.onSuccess({
    case res =>
      tracker.synchronized {

        val information = expression.information()
        val typename = information.source
        val info = Map[String, String]()

        val observation = ProgressiveObservation(tracker.status, res, confidence.getOrElse(0), typename.getOrElse(""), info, t1, System.currentTimeMillis())
        if (!tracker.isCompleted) {
          onComplete(Success(observation))
        }

        log.debug("completed scanning of " + typename.getOrElse("<missing information>"))

        tracker.notifyCompletion(this, observation)
      }
  })
  future.onFailure({
    case res =>
      if (!tracker.isCompleted) {
        onComplete(Failure(res))
      }

      log.error("error when running progressive query", res)
  })

  lazy val confidence: Option[Float] = expression.info.confidence
}