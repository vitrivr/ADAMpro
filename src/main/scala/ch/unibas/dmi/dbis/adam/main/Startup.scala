package ch.unibas.dmi.dbis.adam.main

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object Startup {
  def main(args : Array[String]) {
    SparkStartup
    new Thread(new RPCStartup()).start
  }
}
