package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.evaluation.execution.EvaluationProgressiveQueryPerformer
import ch.unibas.dmi.dbis.adam.evaluation.preparation.{EvaluationDataGenerator, EvaluationIndexCreator}

import scala.concurrent.Future


/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
@deprecated("Should be rewritten to new evaluation framework","2016-03")
object EvaluationOp {
  def generate(): Unit = EvaluationDataGenerator()

  def generate(numberOfElements : Int, numberOfDimensions : Int) : Unit = EvaluationDataGenerator(numberOfElements, numberOfDimensions)

  def index(): Unit = EvaluationIndexCreator()

  /**
   *
   */
  def perform(): Boolean = {
    import scala.concurrent.ExecutionContext.Implicits.global
    Future{
      new EvaluationProgressiveQueryPerformer().start()
    }
    true
  }
}
