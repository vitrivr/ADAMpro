package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.{ProgressiveQueryStatus, ProgressiveQueryStatusTracker}
import ch.unibas.dmi.dbis.adam.query.handler.generic.QueryExpression
import ch.unibas.dmi.dbis.adam.query.information.InformationLevels.LAST_STEP_ONLY
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
class ScanFuture[U](expression: QueryExpression, filter : Option[DataFrame], onComplete: ProgressiveObservation => U, val tracker: ProgressiveQueryStatusTracker)(implicit ac: AdamContext) extends Logging{
  tracker.register(this)

  val t1 = System.currentTimeMillis()

  val future = Future {
    expression.prepareTree().evaluate()
  }
  future.onSuccess({
    case res =>
      tracker.synchronized {

        val information = expression.information(LAST_STEP_ONLY)
        val typename = information.head.source
        val info = Map[String, String]()

        val observation = ProgressiveObservation(tracker.status, res, confidence.getOrElse(0), typename.getOrElse(""), info, t1, System.currentTimeMillis())
        if (tracker.status == ProgressiveQueryStatus.RUNNING) {
          onComplete(observation)
        }

        log.debug("completed scanning of " + typename.getOrElse("<missing information>"))

        tracker.notifyCompletion(this, observation)
      }
  })
  future.onFailure({
    case res =>
      val info = Map[String, String]()
      val observation = ProgressiveObservation(tracker.status, None, confidence.getOrElse(0), res.getMessage, info, t1, System.currentTimeMillis())
      if (tracker.status == ProgressiveQueryStatus.RUNNING) {
        onComplete(observation)
      }

      log.error("error when running progressive query", res)
  })

  lazy val confidence: Option[Float] = expression.info.confidence
}