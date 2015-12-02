package ch.unibas.dmi.dbis.adam.evaluation

import ch.unibas.dmi.dbis.adam.api.GenerateDataOp

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
object EvaluationDataGenerator {
  def apply() {
    EvaluationConfig.dbSizes.foreach { dbSize =>
      EvaluationConfig.vectorSizes.foreach { vecSize =>
        apply(dbSize, vecSize)
      }
    }
  }

  def apply(dbSize : Int, vecSize : Int) {
    try {
      GenerateDataOp("data_" + dbSize + "_" + vecSize, dbSize, vecSize, true)
    } catch {
      case e : Exception => println("Exception")
    }
  }
}
