package ch.unibas.dmi.dbis.adam.query


import scala.collection.mutable.ListBuffer


/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class ProgressiveQueryStatusTracker {
  val futures = ListBuffer[ScanFuture]()
  var returnedGoodResults = false

  def register(future : ScanFuture): Unit ={
    futures.synchronized(futures += future)
  }


  def notifyCompletion(future : ScanFuture): Unit ={
    futures.synchronized({
      if(future.preciseScan){
        returnedGoodResults = true
      }
      futures -= future
    })
  }

  def status = {
    if(futures.isEmpty){
      ProgressiveQueryStatus.FINISHED
    } else {
      ProgressiveQueryStatus.RUNNING
    }
  }
}


object ProgressiveQueryStatus extends Enumeration {
  val FINISHED = Value("finished")
  val RUNNING = Value("running")
}
