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