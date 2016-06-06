package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.QueryExpression
import ch.unibas.dmi.dbis.adam.query.handler.internal.QueryHints.QueryHint
import ch.unibas.dmi.dbis.adam.query.handler.internal.{HintBasedScanExpression, IndexScanExpression, SequentialScanExpression}
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
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
  * Chooses from all index types one (with sequential scan after index scan).
  *
  */
class SimpleProgressivePathChooser()(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    //TODO: choose better default
    IndexTypes.values
      .map(indextypename => Index.list(entityname, indextypename).filter(_.isSuccess).sortBy(-_.get.scanweight))
      .filterNot(_.isEmpty)
      .map(_.head)
      .map(index => {IndexScanExpression(index.get)(nnq)()})
  }
}

/**
  * Chooses all paths (index (without sequential scan) + sequential) for progressive query.
  *
  */
class AllProgressivePathChooser(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    Index.list(entityname)
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
      .map(indextypename => Index.list(entityname, indextypename).filter(_.isSuccess).sortBy(-_.get.scanweight).head)
      .map(index => IndexScanExpression(index.get)(nnq)())
  }
}

/**
  * Chooses first index based on hints given (without sequential scan).
  *
  * @param hints list of QueryHints, note that only IndexQueryHints are accepted at the moment
  */
class QueryHintsProgressivePathChooser(hints: Seq[QueryHint])(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    hints.map(hint => HintBasedScanExpression.startPlanSearch(entityname, Some(nnq), None, Seq(hint))()).filterNot(_ == null)
  }
}

/**
  * Chooses index based on names in given list (without sequential scan).
  *
  * @param indexnames names of indexes
  */
class IndexnameSpecifiedProgressivePathChooser(indexnames: Seq[IndexName])(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    indexnames.map(Index.load(_)).filter(_.isSuccess)
      .map(index => IndexScanExpression(index.get)(nnq)())
  }
}