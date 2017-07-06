package org.vitrivr.adampro.communication.api

import org.vitrivr.adampro.utils.Logging

import scala.util.{Failure, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
abstract class GenericOp extends Logging {
  def execute[T](desc: String)(op: => Try[T]): Try[T] = {
    try {
      log.trace("starting " + desc)
      val t1 = System.currentTimeMillis
      val res = op

      if(res.isFailure){
        throw res.failed.get
      }

      val t2 = System.currentTimeMillis
      log.trace("performed operation '" + desc + "' in " + (t2 - t1) + " ms")
      res
    } catch {
      case e: Exception =>
        log.error("error in operation: " + desc, e)
        Failure(e)
    }
  }
}
