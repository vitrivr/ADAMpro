package org.vitrivr.adampro.query.optimizer

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
class QueryOptimizer(optimizer: OptimizerHeuristic) {
  /**
    *
    * @param ic collection of indexes
    * @param qc collection of queries
    * @param options
    */
  def train(entity : Entity, ic: IndexCollection, qc: QueryCollection, options: Map[String, String]): Try[Void] = {
    try {
      optimizer.trainEntity(entity, qc.getQueries, options)
      optimizer.trainIndexes(ic.getIndexes, qc.getQueries, options)
      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    *
    * @param index
    * @param nnq
    * @return a score which means: the higher the score, the better the index is suited for the given nnq query
    */
  def getScore(index: Index, nnq: NearestNeighbourQuery): Double = {
    optimizer.test(index, nnq)
  }

  /**
    *
    * @param entity
    * @param nnq
    * @return a score which means: the higher the score, the better the entity scan is suited for the given nnq query
    */
  def getScore(entity: Entity, nnq: NearestNeighbourQuery): Double = {
    optimizer.test(entity, nnq)
  }
}


