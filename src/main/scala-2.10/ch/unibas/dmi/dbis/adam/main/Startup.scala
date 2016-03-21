package ch.unibas.dmi.dbis.adam.main

import org.apache.log4j.Logger

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object Startup {
  val log = Logger.getLogger(getClass.getName)

  def main(args : Array[String]) {
    log.debug("ADAMpro starting up")

    SparkStartup
    new Thread(new RPCStartup()).start
  }
}
