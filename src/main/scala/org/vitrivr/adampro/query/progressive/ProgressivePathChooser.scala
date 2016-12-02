package org.vitrivr.adampro.query.progressive

import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.helpers.optimizer.OptimizerOp
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.index.Index._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.QueryHints
import org.vitrivr.adampro.query.handler.generic.QueryExpression
import QueryHints.QueryHint
import org.vitrivr.adampro.query.handler.internal.{HintBasedScanExpression, IndexScanExpression, SequentialScanExpression}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.apache.log4j.Logger

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
/**
  * Specifies which query paths to use in progressive querying
  */
trait ProgressivePathChooser {
  val log = Logger.getLogger(getClass.getName)

  def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression]
}

/**
  * Chooses from all index types one (with sequential scan after index scan) and sequential scan separately.
  *
  */
class SimpleProgressivePathChooser()(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    IndexTypes.values
      .map(indextypename => Index.list(Some(entityname), Some(indextypename)).filter(_.isSuccess).map(_.get)
        .filter(nnq.isConform(_))
        .sortBy(index => - ac.optimizerRegistry.value.apply("naive").get.getScore(index, nnq)))
      .filterNot(_.isEmpty)
      .map(_.head)
      .map(index => {
        IndexScanExpression(index)(nnq)()
      }).+:(new SequentialScanExpression(entityname)(nnq)())
  }
}

/**
  * Chooses all paths (index (without sequential scan) + sequential) for progressive query.
  *
  */
class AllProgressivePathChooser(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    Index.list(Some(entityname))
      .map(index => IndexScanExpression(index.get)(nnq)()).+:(new SequentialScanExpression(entityname)(nnq)())
  }
}

/**
  * Chooses first index based on given index types (without sequential scan).
  *
  * @param indextypenames names of indextypes
  */
class IndexTypeProgressivePathChooser(indextypenames: Seq[IndexTypeName])(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    indextypenames
      .map(indextypename => Index.list(Some(entityname), Some(indextypename)).filter(_.isSuccess).map(_.get)
        .filter(nnq.isConform(_))
        .sortBy(index => - ac.optimizerRegistry.value.apply("naive").get.getScore(index, nnq)).head)
      .map(index => IndexScanExpression(index)(nnq)())
  }
}

/**
  * Chooses first index based on hints given (without sequential scan).
  *
  * @param hints list of QueryHints, note that only IndexQueryHints are accepted at the moment
  */
class QueryHintsProgressivePathChooser(hints: Seq[QueryHint])(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    hints.map(hint => HintBasedScanExpression.startPlanSearch(entityname, Some(nnq), None, Seq(hint), false)()).filterNot(_ == null)
  }
}

/**
  * Chooses index based on names in given list (without sequential scan).
  *
  * @param indexnames names of indexes
  */
class IndexnameSpecifiedProgressivePathChooser(indexnames: Seq[IndexName])(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    //here we do not filter for query conformness, as the user has specified explicitly some index names
    //and should get an exception otherwise
    indexnames.map(Index.load(_)).map(_.get)
      .map(index => IndexScanExpression(index)(nnq)())
  }
}