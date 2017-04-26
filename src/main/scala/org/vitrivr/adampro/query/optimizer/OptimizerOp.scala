package org.vitrivr.adampro.query.optimizer

import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity.{AttributeName, EntityName}
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.query.handler.generic.QueryExpression
import org.vitrivr.adampro.query.handler.internal.{IndexScanExpression, SequentialScanExpression}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery


/**
  * ADAMpro
  *
  * Ivan Giangreco
  * April 2017
  */
case class ExecutionPath(expr : QueryExpression, scan : String, score : Double)


object OptimizerOp {

  /**
    * Returns all scans scored under the optimizer given the accessible scan possibilities (index, entity, etc.), the query
    *
    * @param optimizerName
    * @param entityname
    * @param nnq
    * @param filterExpr
    * @param ac
    * @return
    */
  def scoredScans(optimizerName : String, entityname : EntityName, nnq: NearestNeighbourQuery)(filterExpr: Option[QueryExpression] = None)(implicit ac: AdamContext): Seq[ExecutionPath] = {
    val optimizer = ac.optimizerRegistry.value.apply(optimizerName).get

    val indexes = SparkStartup.catalogOperator.listIndexes(Some(entityname), Some(nnq.attribute)).get.map(Index.load(_)).filter(_.isSuccess).map(_.get).groupBy(_.indextypename).mapValues(_.map(_.indexname))
    val indexScans = indexes.values.toSeq.flatten
      .map(indexname => Index.load(indexname, false).get)
      .map(index => {
        val score = optimizer.getScore(index, nnq)

        ExecutionPath(IndexScanExpression(index)(nnq, None)(filterExpr)(ac), index.indexname,score)
      })

    val entity = Entity.load(entityname).get
    val entityScan = {
        val score = optimizer.getScore(entity, nnq)

        ExecutionPath(SequentialScanExpression(entity)(nnq, None)(filterExpr)(ac), entity.entityname, score)
      }

    indexScans ++ Seq(entityScan)
  }


  /**
    * Returns the optimal scan under the optimizer given the accessible scan possibilities (index, entity, etc.), the query
    *
    * @param optimizerName
    * @param entityname
    * @param nnq
    * @param filterExpr
    * @param ac
    * @return
    */
  def getOptimalScan(optimizerName : String, entityname : EntityName, nnq: NearestNeighbourQuery)(filterExpr: Option[QueryExpression] = None)(implicit ac: AdamContext) : QueryExpression = {
    scoredScans(optimizerName, entityname, nnq)(filterExpr)(ac).sortBy(x => -x.score).head.expr
  }


}
