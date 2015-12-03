package ch.unibas.dmi.dbis.adam.evaluation.execution

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import com.typesafe.config.ConfigFactory

/**
 * adamtwo
 *
 * Ivan Giangreco
 * December 2015
 */
object EvaluationPerformanceStartup {
    val config: AdamConfig = new AdamConfig(ConfigFactory.load())

    def main(args : Array[String]) {
      SparkStartup

      val ep = new EvaluationProgressiveQueryPerformer()
      ep.init()
      ep.start()
    }
  }
