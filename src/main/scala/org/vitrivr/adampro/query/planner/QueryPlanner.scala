package org.vitrivr.adampro.query.planner

import org.vitrivr.adampro.data.entity.Entity
import org.vitrivr.adampro.data.index.Index
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.query.RankingQuery

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class QueryPlanner(heuristic: PlannerHeuristics)(@transient implicit val ac: SharedComponentContext)  {
  /**
    *
    * @param ic collection of indexes
    * @param qc collection of queries
    * @param options
    */
  def train(entity : Entity, ic: IndexCollection, qc: QueryCollection, options: Map[String, String]): Try[Void] = {
    try {
      heuristic.trainEntity(entity, qc.getQueries, options)
      heuristic.trainIndexes(ic.getIndexes, qc.getQueries, options)
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
  def getScore(index: Index, nnq: RankingQuery): Double = {
    heuristic.test(index, nnq)
  }

  /**
    *
    * @param entity
    * @param nnq
    * @return a score which means: the higher the score, the better the entity scan is suited for the given nnq query
    */
  def getScore(entity: Entity, nnq: RankingQuery): Double = {
    heuristic.test(entity, nnq)
  }
}


