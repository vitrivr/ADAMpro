package ch.unibas.dmi.dbis.adam.evaluation

import ch.unibas.dmi.dbis.adam.api.IndexOp
import ch.unibas.dmi.dbis.adam.query.distance.ManhattanDistance

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
object EvaluationIndexCreator {
  def apply() {
    EvaluationConfig.dbSizes.foreach { dbSize =>
      EvaluationConfig.vectorSizes.foreach { vecSize =>
        EvaluationConfig.indexes.foreach { indextypename =>
          IndexOp("data_" + dbSize + "_" + vecSize, indextypename, ManhattanDistance, Map[String, String]())
        }
      }
    }
  }
}
