package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.evaluation.{EvaluationDataGenerator, EvaluationIndexCreator, EvaluationPerformer}

import scala.concurrent.Future


/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
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
      new EvaluationPerformer().start()
    }
    true
  }
}
