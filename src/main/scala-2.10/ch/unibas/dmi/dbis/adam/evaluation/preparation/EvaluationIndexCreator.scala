package ch.unibas.dmi.dbis.adam.evaluation.preparation

import ch.unibas.dmi.dbis.adam.api.IndexOp
import ch.unibas.dmi.dbis.adam.evaluation.EvaluationConfig
import ch.unibas.dmi.dbis.adam.index.Index
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

          val entityname = "data_" + dbSize + "_" + vecSize

          if(Index.getIndexnames(entityname).isEmpty){
            IndexOp(entityname, indextypename, ManhattanDistance, Map[String, String]())
          }

        }
      }
    }
  }
}
