package org.vitrivr.adampro.query.execution

/**
  * adamtwo
  *
  * Tracks the status of a progressively running query.
  *
  * Ivan Giangreco
  * September 2015
  */
object ProgressiveQueryStatus extends Enumeration {
  val RUNNING = Value("running")
  val PREMATURE_FINISHED = Value("premature")
  val FINISHED = Value("finished")
}
