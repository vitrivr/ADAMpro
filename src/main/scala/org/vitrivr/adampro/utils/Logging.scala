package org.vitrivr.adampro.utils

import org.slf4j.{MarkerFactory, Logger, LoggerFactory}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
trait Logging {
  protected final val QUERY_MARKER = MarkerFactory.getMarker("QUERY_MARKER")
  @transient private var log_ : Logger = null


  /**
    *
    * @param thunk
    * @tparam T
    * @return
    */
  def time[T](desc: String)(thunk: => T): T = {
    val t1 = System.currentTimeMillis
    log.trace(desc + " : " + "started at " + t1)
    val x = thunk
    val t2 = System.currentTimeMillis
    log.trace(desc + " : " + "started at " + t1 + " finished at " + t2)
    log.debug(desc + " : " + (t2 - t1) + " msecs")
    x
  }

  /**
    *
    * @return
    */
  protected def logName = {
    this.getClass.getName.stripSuffix("$")
  }

  /**
    *
    * @return
    */
  protected def log: Logger = {
    if (log_ == null) {
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }
}