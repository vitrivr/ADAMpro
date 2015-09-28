package ch.unibas.dmi.dbis.adam.query

import scala.collection.mutable.Map

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class ProgressiveQueryStatus() {
  val status = Map[String, Boolean]()

  def startAll(elements : Seq[String])={
    elements.foreach{
      element => start(element)
    }
  }

  def start(element : String)={
    status += element -> false
  }

  def end(element : String) = {
    status(element) = true
  }

  def allEnded = status.values.reduce(_ && _)
}
