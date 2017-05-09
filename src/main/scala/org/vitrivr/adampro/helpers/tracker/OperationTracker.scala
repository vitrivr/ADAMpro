package org.vitrivr.adampro.helpers.tracker

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.query.progressive.ProgressiveObservation

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * March 2017
  */
case class OperationTracker(onComplete : Option[(Try[ProgressiveObservation]) => Unit] = None) {
  private val bcLb = new ListBuffer[Broadcast[_]]()

  /**
    * Add a broadcast variable.
    *
    * @param bc
    */
  def addBroadcast(bc: Broadcast[_]): Unit = {
    bcLb += bc
  }

  /**
    * Clean tracks of operation.
    */
  def cleanAll(): Unit = {
    bcLb.foreach { bc =>
      bc.destroy()
    }

    bcLb.clear()
  }

  /**
    *
    * @param observation
    */
  def addResult(observation : Try[ProgressiveObservation]) : Unit = {
    if(onComplete.isDefined){
      onComplete.get(observation)
    }
  }
}
