package ch.unibas.dmi.dbis.adam.main


import ch.unibas.dmi.dbis.adam.config.AdamConfig
import com.typesafe.config.ConfigFactory

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object Startup {
  val config: AdamConfig = new AdamConfig(ConfigFactory.load())

  def main(args : Array[String]) {
    SparkStartup
    new Thread(new CLIStartup(config)).start
  }
}
