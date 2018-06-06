package org.vitrivr.adampro.utils

import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.LogManager


/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
trait Logging {
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
      log_ = LogManager.getLogger(logName)
    }
    log_
  }
}