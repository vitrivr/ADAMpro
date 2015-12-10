package ch.unibas.dmi.dbis.adam.evaluation.preparation

import ch.unibas.dmi.dbis.adam.api.GenerateDataOp
import ch.unibas.dmi.dbis.adam.evaluation.EvaluationConfig

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
      case e : Exception => e.printStackTrace()
    }
  }
}
