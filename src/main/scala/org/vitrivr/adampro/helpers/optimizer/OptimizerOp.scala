package org.vitrivr.adampro.helpers.optimizer

import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.query.query.NearestNeighbourQuery

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class OptimizerOp(benchmarker : OptimizerHeuristic) {
  /**
    *
    * @param ic collection of indexes
    * @param qc collection of queries
    */
  def train(ic: IndexCollection, qc: QueryCollection): Try[Void] = {
    try {
      benchmarker.train(ic.getIndexes, qc.getQueries)
      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    *
    * @param index
    * @param nnq
    * @return
    */
  def getScore(index: Index, nnq: NearestNeighbourQuery): Double = {
    benchmarker.test(index, nnq)
  }

  /**
    *
    * @param entity
    * @param nnq
    * @return
    */
  def getScore(entity: Entity, nnq: NearestNeighbourQuery): Double = {
    benchmarker.test(entity, nnq)
  }
}
