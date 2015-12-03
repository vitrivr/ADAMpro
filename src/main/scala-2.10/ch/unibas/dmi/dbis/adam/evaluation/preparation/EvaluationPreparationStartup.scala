package ch.unibas.dmi.dbis.adam.evaluation.preparation

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import com.typesafe.config.ConfigFactory

/**
 * adamtwo
 *
 * Ivan Giangreco
 * December 2015
 */
object EvaluationPreparationStartup {
  val config: AdamConfig = new AdamConfig(ConfigFactory.load())

  def main(args : Array[String]) {
    SparkStartup
    EvaluationDataGenerator()
    EvaluationIndexCreator()
  }
}
