package ch.unibas.dmi.dbis.adam.main

import ch.unibas.dmi.dbis.adam.cli.CLI
import ch.unibas.dmi.dbis.adam.config.AdamConfig
import scala.tools.nsc.Settings
import com.typesafe.config.ConfigFactory

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object Startup {
  val config = new AdamConfig(ConfigFactory.load())

  val settings = new Settings
  settings.usejavacp.value = true
  settings.deprecation.value = true

  def main(args : Array[String]) {
    SparkStartup

    new CLI().process(settings)
  }
}
