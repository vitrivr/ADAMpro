package org.vitrivr.adampro.query.tracker

import org.vitrivr.adampro.query.ast.generic.QueryExpression
import org.vitrivr.adampro.query.ast.internal._
import org.vitrivr.adampro.query.execution.ProgressiveObservation
import org.vitrivr.adampro.query.query.InformationLevels

import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
case class ResultTracker(onNext: (Try[ProgressiveObservation]) => Unit, level: InformationLevels.InformationLevel) {

  /**
    * Add progressive observation result of a progressive query.
    *
    * @param source      source of observation
    * @param observation observation
    */
  def addObservation(source: QueryExpression, observation: Try[ProgressiveObservation]): Unit = {
    if (observation.isFailure) {
      //all failures are sent to observer
      onNext(observation)
    } else {
      //observation is success

      //depending on level, different information in sent to observer
      if (level == InformationLevels.LAST_STEP_ONLY) {
        if (source.isInstanceOf[IndexScanExpression] || source.isInstanceOf[SequentialScanExpression]
          || source.isInstanceOf[StochasticIndexQueryExpression] || source.isInstanceOf[TimedScanExpression] ||
          source.isInstanceOf[ProjectionExpression] || source == BooleanFilterExpression) {
          //do not process all entering observations
          onNext(observation)
        }
      } else {
        onNext(observation)
      }
    }
  }
}
