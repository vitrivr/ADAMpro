package ch.unibas.dmi.dbis.adam.query


import ch.unibas.dmi.dbis.adam.main.SparkStartup

import scala.collection.mutable.ListBuffer


/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class ProgressiveQueryStatusTracker(queryID : String) {
  private val futures = ListBuffer[ScanFuture]()
  private var runningStatus = ProgressiveQueryStatus.RUNNING
  private var resultConfidence = 0.toFloat
  private var results = Seq[Result]()

  /**
   *
   * @param future
   */
  def register(future : ScanFuture): Unit ={
    futures.synchronized(futures += future)
  }

  /**
   *
   * @param future
   */
  def notifyCompletion(future : ScanFuture, futureResults : Seq[Result]): Unit ={
    futures.synchronized({
      if(future.confidence > resultConfidence && runningStatus == ProgressiveQueryStatus.RUNNING){
        results = futureResults
        resultConfidence = future.confidence
      }

      if(resultConfidence == 1.0){
        stop(ProgressiveQueryStatus.FINISHED)
      }

      futures -= future
    })
  }

  /**
   *
   * @return
   */
  def getResults() = (results, resultConfidence)

  /**
   *
   */
  def stop() : Unit = {
    stop(ProgressiveQueryStatus.PREMATURE_FINISHED)
  }

  /**
   * 
   * @param status
   */
  private def stop(status : ProgressiveQueryStatus.Value) : Unit = {
    futures.synchronized {
      SparkStartup.sc.cancelJobGroup(queryID)
      runningStatus = status
      futures.clear()
    }
  }


    /**
   *
   * @return
   */
  def getStatus() = {
      futures.synchronized {
        runningStatus
      }
  }
}

/**
 *
 */
object ProgressiveQueryStatus extends Enumeration {
  val RUNNING = Value("running")
  val PREMATURE_FINISHED = Value("premature")
  val FINISHED = Value("finished")
}
