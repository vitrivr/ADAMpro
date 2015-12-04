package ch.unibas.dmi.dbis.adam.evaluation.preparation

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.main.SparkStartup

/**
 * adamtwo
 *
 * Ivan Giangreco
 * December 2015
 */
object EvaluationPreparationStartup {
  def main(args : Array[String]) {
    AdamConfig.evaluation = true

    SparkStartup
    EvaluationDataGenerator()
    EvaluationIndexCreator()
  }
}
