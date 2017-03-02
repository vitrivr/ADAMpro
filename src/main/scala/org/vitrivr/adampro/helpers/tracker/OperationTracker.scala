package org.vitrivr.adampro.helpers.tracker

import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ListBuffer

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * March 2017
  */
case class OperationTracker() {
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
}
