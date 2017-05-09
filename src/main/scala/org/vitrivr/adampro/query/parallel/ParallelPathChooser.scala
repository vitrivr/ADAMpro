package org.vitrivr.adampro.query.parallel

import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.index.Index._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.QueryHints.{QueryHint, SimpleQueryHint}
import org.vitrivr.adampro.query.handler.generic.QueryExpression
import org.vitrivr.adampro.query.handler.internal.{HintBasedScanExpression, IndexScanExpression, SequentialScanExpression}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
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

  def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression]
}

/**
  * Chooses from all index types one (with sequential scan after index scan) and sequential scan separately.
  *
  */
class SimpleParallelPathChooser()(implicit ac: AdamContext) extends ParallelPathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    IndexTypes.values
      .map(indextypename => Index.list(Some(entityname), Some(nnq.attribute), Some(indextypename)).filter(_.isSuccess).map(_.get)
        .filter(nnq.isConform(_))
        .sortBy(index => - ac.optimizerRegistry.value.apply("naive").get.getScore(index, nnq)))
      .filterNot(_.isEmpty)
      .map(_.head)
      .map(index => {
        IndexScanExpression(index)(nnq, None)()(ac)
      }).+:(new SequentialScanExpression(entityname)(nnq, None)(None)(ac))
  }
}

/**
  * Chooses all paths (index (without sequential scan) + sequential) for parallel query.
  *
  */
class AllParallelPathChooser(implicit ac: AdamContext) extends ParallelPathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    Index.list(Some(entityname))
      .map(index => IndexScanExpression(index.get)(nnq, None)(None)(ac)).+:(new SequentialScanExpression(entityname)(nnq, None)(None)(ac))
  }
}

/**
  * Chooses first index based on given index types (without sequential scan).
  *
  * @param indextypenames names of indextypes
  */
class IndexTypeParallelPathChooser(indextypenames: Seq[IndexTypeName])(implicit ac: AdamContext) extends ParallelPathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    indextypenames
      .map(indextypename => Index.list(Some(entityname), Some(nnq.attribute), Some(indextypename)).filter(_.isSuccess).map(_.get)
        .filter(nnq.isConform(_))
        .sortBy(index => - ac.optimizerRegistry.value.apply("naive").get.getScore(index, nnq)).head)
      .map(index => IndexScanExpression(index)(nnq, None)(None)(ac))
  }
}

/**
  * Chooses path based on hints given.
  *
  * @param hints list of QueryHints, note that only Simple are accepted at the moment
  */
class QueryHintsParallelPathChooser(hints: Seq[QueryHint])(implicit ac: AdamContext) extends ParallelPathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    if(hints.filter(x => !x.isInstanceOf[SimpleQueryHint]).length > 0){
      log.warn("only index query hints allowed in path chooser")
      Seq()
    } else {
      hints.map(hint => HintBasedScanExpression.startPlanSearch(entityname, Some(nnq), None, Seq(hint), false)()).filterNot(_ == null)
    }
  }
}

/**
  * Chooses index based on names in given list (without sequential scan).
  *
  * @param indexnames names of indexes
  */
class IndexnameSpecifiedParallelPathChooser(indexnames: Seq[IndexName])(implicit ac: AdamContext) extends ParallelPathChooser {
  override def getPaths(entityname: EntityName, nnq: NearestNeighbourQuery): Seq[QueryExpression] = {
    //here we do not filter for query conformness, as the user has specified explicitly some index names
    //and should get an exception otherwise
    indexnames.map(Index.load(_)).map(_.get)
      .map(index => IndexScanExpression(index)(nnq, None)(None)(ac))
  }
}