package org.vitrivr.adampro.query.progressive

import org.vitrivr.adampro.config.AdamConfig
import org.vitrivr.adampro.main.SharedComponentContext
import org.vitrivr.adampro.query.parallel.ScanFuture
import org.vitrivr.adampro.utils.Logging

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{CanAwait, ExecutionContext, Future}
import scala.util.{Success, Try}


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
