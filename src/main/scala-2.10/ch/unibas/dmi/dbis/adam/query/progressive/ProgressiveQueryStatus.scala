package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.Result

import scala.collection.mutable.ListBuffer


/**
  * adamtwo
  *
  * Tracks the status of a progressively running query.
  *
  * Ivan Giangreco
  * September 2015
  */
class ProgressiveQueryStatusTracker(queryID: String) {
  private val futures = ListBuffer[ScanFuture]()
  private var runningStatus = ProgressiveQueryStatus.RUNNING
  private var resultConfidence = 0.toFloat
  private var queryResults = Seq[Result]()

  /**
    * Register a scan future.
    *
    * @param future
    */
  def register(future: ScanFuture): Unit = futures.synchronized(futures += future)

  /**
    * Notifies the tracker of its completion.
    *
    * @param future
    */
  def notifyCompletion(future: ScanFuture, futureResults: Seq[Result]): Unit = {
    futures.synchronized({
      if (future.confidence > resultConfidence && runningStatus == ProgressiveQueryStatus.RUNNING) {
        queryResults = futureResults
        resultConfidence = future.confidence
      }

      if (!AdamConfig.evaluation) {
        // in evaluation mode we want to keep all results and do not stop, as to be able to measure how
        // much time each index would run
        if (math.abs(resultConfidence - 1.0) < 0.000001) {
          stop(ProgressiveQueryStatus.FINISHED)
        }
      }

      futures -= future

      if (futures.isEmpty) {
        stop(ProgressiveQueryStatus.FINISHED)
      }
    })
  }

  /**
    * Prematurely stops the progressive query.
    */
  def stop(): Unit = stop(ProgressiveQueryStatus.PREMATURE_FINISHED)

  /**
    * Stops the progressive query with the new status.
    * @param newStatus
    */
  private def stop(newStatus: ProgressiveQueryStatus.Value): Unit = {
    if (runningStatus == ProgressiveQueryStatus.FINISHED) { //already finished
      return
    }

    futures.synchronized {
      SparkStartup.sc.cancelJobGroup(queryID)
      runningStatus = newStatus
      futures.clear()
    }
  }

  /**
    * Returns the most up-to-date results together with a confidence score.
    * @return
    */
  def results = (queryResults, resultConfidence)

  /**
    * Returns the current status of the progressive query.
    * @return
    */
  def status = futures.synchronized {
    runningStatus
  }

  /**
    * Returns the number of current queries running in parallel.
    *
    * @return
    */
  def length = futures.size
}

/**
  *
  */
object ProgressiveQueryStatus extends Enumeration {
  val RUNNING = Value("running")
  val PREMATURE_FINISHED = Value("premature")
  val FINISHED = Value("finished")
}
