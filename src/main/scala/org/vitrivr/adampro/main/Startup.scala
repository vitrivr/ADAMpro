package org.vitrivr.adampro.main

import org.apache
import org.apache.spark

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object Startup {
  def main(args : Array[String]) {
    SparkStartup
    new Thread(new RPCStartup(SparkStartup.mainContext.config.grpcPort)).start
  }
}
