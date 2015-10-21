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
      if(runningStatus == ProgressiveQueryStatus.RUNNING){
        results = futureResults
      }

      if(future.preciseScan){
        stop(ProgressiveQueryStatus.FINISHED)
      }
      futures -= future
    })
  }

  /**
   *
   * @return
   */
  def getResults() = results //TODO: return with confidence score?

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
    SparkStartup.sc.cancelJobGroup(queryID)
    runningStatus = status
    futures.clear()
  }


    /**
   *
   * @return
   */
  def getStatus = {
    if(futures.isEmpty && runningStatus == ProgressiveQueryStatus.RUNNING){
      ProgressiveQueryStatus.PREMATURE_FINISHED
    } else {
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
