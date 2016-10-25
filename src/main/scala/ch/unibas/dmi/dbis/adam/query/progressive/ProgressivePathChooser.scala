package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.helpers.benchmark.ScanWeightCatalogOperator
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.QueryHints
import ch.unibas.dmi.dbis.adam.query.handler.generic.QueryExpression
import QueryHints.QueryHint
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
  * Chooses from all index types one (with sequential scan after index scan) and sequential scan separately.
  *
  */
class SimpleProgressivePathChooser()(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    IndexTypes.values
      .map(indextypename => Index.list(Some(entityname), Some(indextypename)).filter(_.isSuccess).map(_.get)
        .filter(nnq.isConform(_))
        .sortBy(index => -ScanWeightCatalogOperator(index)))
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
        .sortBy(index => -ScanWeightCatalogOperator(index)).head)
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