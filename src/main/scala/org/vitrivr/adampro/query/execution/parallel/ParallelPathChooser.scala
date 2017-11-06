package org.vitrivr.adampro.query.execution.parallel

import org.vitrivr.adampro.data.datatypes.vector.Vector.MathVector
import org.vitrivr.adampro.data.entity.Entity.{AttributeName, EntityName}
import org.vitrivr.adampro.data.index.Index
import org.vitrivr.adampro.data.index.Index._
import org.vitrivr.adampro.data.index.structures.IndexTypes
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.query.QueryHints.{QueryHint, SimpleQueryHint}
import org.vitrivr.adampro.query.ast.generic.QueryExpression
import org.vitrivr.adampro.query.ast.internal.{HintBasedScanExpression, IndexScanExpression, SequentialScanExpression}
import org.vitrivr.adampro.query.distance.DistanceFunction
import org.vitrivr.adampro.query.planner.PlannerRegistry
import org.vitrivr.adampro.query.query.RankingQuery
import org.vitrivr.adampro.utils.Logging

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
/**
  * Specifies which query paths to use in parallel querying
  */
trait ParallelPathChooser extends Logging {

  def getPaths(entityname: EntityName, nnq: RankingQuery): Seq[QueryExpression]
}

/**
  * Chooses from all index types one (with sequential scan after index scan) and sequential scan separately.
  *
  */
class SimpleParallelPathChooser()(implicit ac: SharedComponentContext) extends ParallelPathChooser {
  override def getPaths(entityname: EntityName, rq: RankingQuery): Seq[QueryExpression] = {
    val nnq = new RankingQuery(rq.attribute, rq.q, rq.weights, rq.distance, rq.k, true, rq.options, rq.partitions, rq.queryID)

    val scans = IndexTypes.values
      .map(indextypename => Index.list(Some(entityname), Some(nnq.attribute), Some(indextypename)).filter(_.isSuccess).map(_.get)
        .filter(nnq.isConform(_))
        .sortBy(index => - PlannerRegistry.apply("naive").get.getScore(index, nnq)))
      .filterNot(_.isEmpty)
      .map(_.head)
      .map(index => {
        IndexScanExpression(index)(nnq, None)()(ac)
      })

    if(!rq.indexOnly){
      scans.+:(new SequentialScanExpression(entityname)(nnq, None)(None)(ac))
    } else {
     scans
    }
  }
}

/**
  * Chooses all paths (index (without sequential scan) + sequential) for parallel query.
  *
  */
class AllParallelPathChooser(implicit ac: SharedComponentContext) extends ParallelPathChooser {
  override def getPaths(entityname: EntityName, rq: RankingQuery): Seq[QueryExpression] = {
    val nnq = new RankingQuery(rq.attribute, rq.q, rq.weights, rq.distance, rq.k, true, rq.options, rq.partitions, rq.queryID)

    val scans = Index.list(Some(entityname))
      .map(index => IndexScanExpression(index.get)(nnq, None)(None)(ac))

    if(!rq.indexOnly){
      scans.+:(new SequentialScanExpression(entityname)(nnq, None)(None)(ac))
    } else {
      scans
    }
  }
}

/**
  * Chooses first index based on given index types (without sequential scan).
  *
  * @param indextypenames names of indextypes
  */
class IndexTypeParallelPathChooser(indextypenames: Seq[IndexTypeName])(implicit ac: SharedComponentContext) extends ParallelPathChooser {
  override def getPaths(entityname: EntityName, rq: RankingQuery): Seq[QueryExpression] = {
    val nnq = new RankingQuery(rq.attribute, rq.q, rq.weights, rq.distance, rq.k, true, rq.options, rq.partitions, rq.queryID)

    val scans = indextypenames
      .map(indextypename => Index.list(Some(entityname), Some(nnq.attribute), Some(indextypename)).filter(_.isSuccess).map(_.get)
        .filter(nnq.isConform(_))
        .sortBy(index => - PlannerRegistry.apply("naive").get.getScore(index, nnq)).head)
      .map(index => IndexScanExpression(index)(nnq, None)(None)(ac))

    scans
  }
}

/**
  * Chooses path based on hints given.
  *
  * @param hints list of QueryHints, note that only Simple are accepted at the moment
  */
class QueryHintsParallelPathChooser(hints: Seq[QueryHint])(implicit ac: SharedComponentContext) extends ParallelPathChooser {
  override def getPaths(entityname: EntityName, rq: RankingQuery): Seq[QueryExpression] = {
    val nnq = new RankingQuery(rq.attribute, rq.q, rq.weights, rq.distance, rq.k, true, rq.options, rq.partitions, rq.queryID)

    val scans = if(hints.filter(x => !x.isInstanceOf[SimpleQueryHint]).length > 0){
      log.warn("only index query hints allowed in path chooser")
      Seq()
    } else {
      hints.map(hint => HintBasedScanExpression.startPlanSearch(entityname, Some(nnq), None, Seq(hint), false)()).filterNot(_ == null)
    }

    if(!rq.indexOnly){
      scans.+:(new SequentialScanExpression(entityname)(nnq, None)(None)(ac))
    } else {
      scans
    }
  }
}

/**
  * Chooses index based on names in given list (without sequential scan).
  *
  * @param indexnames names of indexes
  */
class IndexnameSpecifiedParallelPathChooser(indexnames: Seq[IndexName])(implicit ac: SharedComponentContext) extends ParallelPathChooser {
  override def getPaths(entityname: EntityName, rq: RankingQuery): Seq[QueryExpression] = {
    val nnq = new RankingQuery(rq.attribute, rq.q, rq.weights, rq.distance, rq.k, true, rq.options, rq.partitions, rq.queryID)

    //here we do not filter for query conformness, as the user has specified explicitly some index names
    //and should get an exception otherwise
    indexnames.map(Index.load(_)).map(_.get)
      .map(index => IndexScanExpression(index)(nnq, None)(None)(ac))
  }
}