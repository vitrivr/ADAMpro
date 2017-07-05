package org.vitrivr.adampro.query.tracker

import org.apache.spark.broadcast.Broadcast
import org.vitrivr.adampro.query.ast.generic.QueryExpression
import org.vitrivr.adampro.query.execution.ProgressiveObservation

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * March 2017
  */
case class QueryTracker(resultTracker: Option[ResultTracker] = None) {
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
    * Add a progressive observation.
    *
    * @param observation
    */
  def addObservation(source : QueryExpression, observation : Try[ProgressiveObservation]) : Unit = {
    if(resultTracker.isDefined){
      resultTracker.get.addObservation(source, observation)
    }
  }
}
