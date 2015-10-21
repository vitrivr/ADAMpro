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
  private var returnedGoodResults = false

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
  def notifyCompletion(future : ScanFuture): Unit ={
    futures.synchronized({
      if(future.preciseScan){
        stop()
        returnedGoodResults = true
      }
      futures -= future
    })
  }


  /**
   *
   */
  def stop() : Unit = {
    SparkStartup.sc.cancelJobGroup(queryID)
    futures.clear()
  }


  /**
   *
   * @return
   */
  def status = {
    if(futures.isEmpty || returnedGoodResults){
      ProgressiveQueryStatus.FINISHED
    } else {
      ProgressiveQueryStatus.RUNNING
    }
  }
}

/**
 *
 */
object ProgressiveQueryStatus extends Enumeration {
  val FINISHED = Value("finished")
  val RUNNING = Value("running")
}
