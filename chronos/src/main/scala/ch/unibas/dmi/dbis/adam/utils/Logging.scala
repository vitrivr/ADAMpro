package ch.unibas.dmi.dbis.adam.utils

import org.slf4j.{Logger, LoggerFactory}

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
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }
}