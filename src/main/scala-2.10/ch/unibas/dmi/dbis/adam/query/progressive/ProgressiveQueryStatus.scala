package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.Result

import scala.collection.mutable.ListBuffer


/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class ProgressiveQueryStatusTracker(queryID: String) {
  private val futures = ListBuffer[ScanFuture]()
  private var runningStatus = ProgressiveQueryStatus.RUNNING
  private var resultConfidence = 0.toFloat
  private var queryResults = Seq[Result]()

  def register(future: ScanFuture): Unit = futures.synchronized(futures += future)

  /**
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
        // in evaluation mode we want to keep all results and do not stop
        if (math.abs(resultConfidence - 1.0) < 0.000001) {
          stop(ProgressiveQueryStatus.FINISHED)
        }
      }

      futures -= future

      if(futures.isEmpty){
        stop(ProgressiveQueryStatus.FINISHED)
      }
    })
  }

  def stop(): Unit = stop(ProgressiveQueryStatus.PREMATURE_FINISHED)

  private def stop(newStatus: ProgressiveQueryStatus.Value): Unit = {
    if (runningStatus == ProgressiveQueryStatus.FINISHED) {
      return
    }

    futures.synchronized {
      SparkStartup.sc.cancelJobGroup(queryID)
      runningStatus = newStatus
      futures.clear()
    }
  }

  def results = (queryResults, resultConfidence)

  def status = futures.synchronized {
    runningStatus
  }

  def numberOfFutures = futures.size
}

/**
 *
 */
object ProgressiveQueryStatus extends Enumeration {
  val RUNNING = Value("running")
  val PREMATURE_FINISHED = Value("premature")
  val FINISHED = Value("finished")
}
