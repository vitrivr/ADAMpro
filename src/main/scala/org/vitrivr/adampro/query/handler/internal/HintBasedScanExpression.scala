package org.vitrivr.adampro.query.handler.internal

import org.vitrivr.adampro.catalog.CatalogOperator
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity._
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.index.Index._
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.query.QueryHints
import org.vitrivr.adampro.query.handler.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.handler.internal.AggregationExpression.EmptyExpression
import org.vitrivr.adampro.query.handler.internal.BooleanFilterExpression.BooleanFilterScanExpression
import QueryHints._
import org.vitrivr.adampro.query.query.{BooleanQuery, NearestNeighbourQuery}
import org.vitrivr.adampro.query.optimizer.OptimizerOp
import org.vitrivr.adampro.utils.Logging
import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.helpers.tracker.OperationTracker

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class HintBasedScanExpression(private val entityname: EntityName, private val nnq: Option[NearestNeighbourQuery], private val bq: Option[BooleanQuery], private val hints: Seq[QueryHint], private val withFallback: Boolean = true, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  var expr = HintBasedScanExpression.startPlanSearch(entityname, nnq, bq, hints, withFallback)(filterExpr)
  override val info = ExpressionDetails(expr.info.source, Some("Hint-Based Expression: " + expr.info.scantype), id, expr.info.confidence)
  _children ++= Seq(expr) ++ filterExpr.map(Seq(_)).getOrElse(Seq())

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : OperationTracker)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("evaluate hint-based expression, scanning " + expr.info.scantype)
    expr.filter = filter
    expr.evaluate(options)(tracker)
  }

  override def prepareTree(): QueryExpression = {
    super.prepareTree()
    expr = expr.prepareTree()
    this
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: HintBasedScanExpression =>
        this.entityname.equals(that.entityname) &&
          this.nnq.isDefined == that.nnq.isDefined &&
          this.nnq.map(nnq1 => that.nnq.map(nnq2 => nnq1.equals(nnq2)).getOrElse(false)).getOrElse(true) &&
          this.bq.map(bq1 => that.bq.map(bq2 => bq1.equals(bq2)).getOrElse(false)).getOrElse(true) &&
          this.expr.equals(that.expr)
      case _ => expr.equals(that)
    }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + entityname.hashCode
    result = prime * result + nnq.map(_.hashCode).getOrElse(0)
    result = prime * result + bq.map(_.hashCode).getOrElse(0)
    result = prime * result + expr.hashCode
    result
  }
}

object HintBasedScanExpression extends Logging {

  def startPlanSearch(entityname: EntityName, nnq: Option[NearestNeighbourQuery], bq: Option[BooleanQuery], hints: Seq[QueryHint], withFallback: Boolean = true)(expr: Option[QueryExpression] = None)(implicit ac: AdamContext): QueryExpression = {
    var plan = getPlan(entityname, nnq, bq, hints)(expr)

    if (plan.isEmpty) {
      if (withFallback) {
        log.warn("no query plan chosen, go to fallback")
        plan = getPlan(entityname, nnq, bq, Seq(QueryHints.FALLBACK_HINTS))(expr)
      } else {
        throw new GeneralAdamException("using hints no execution plan could be found, using empty plan")
      }
    }

    log.debug("using plan: " + plan.get.getClass.getName)

    plan.get

  }

  /**
    * Chooses the query plan to use based on the given hint, the available indexes, etc.
    *
    * @param entityname name of entity
    * @param nnq        nearest neighbour query
    * @param bq         boolean query
    * @param hints      query hints
    * @param expr       filter expression
    * @return
    */
  private def getPlan(entityname: EntityName, nnq: Option[NearestNeighbourQuery], bq: Option[BooleanQuery], hints: Seq[QueryHint])(expr: Option[QueryExpression] = None)(implicit ac: AdamContext): Option[QueryExpression] = {
    if (hints.isEmpty) {
      log.trace("no execution plan hint")
      return None
    }

    if (nnq.isEmpty && bq.isEmpty) {
      log.trace("no search expressions defined")
      return None
    }

    var scan: Option[QueryExpression] = None
    if (bq.isDefined) {
      scan = Some(new BooleanFilterScanExpression(entityname)(bq.get, None)(expr)(ac))
    }

    if (nnq.isEmpty) {
      return scan
    } else {
      var j = 0
      while (j < hints.length) {
        hints(j) match {
          case EMPIRICAL =>
            //TODO: clean code duplication
            log.trace("measurement-based (empirical) execution plan hint")
            val optimizer = ac.optimizerRegistry.value.apply("svm").get

            val indexes = SparkStartup.catalogOperator.listIndexes(Some(entityname), Some(nnq.get.attribute)).get.map(Index.load(_)).filter(_.isSuccess).map(_.get).groupBy(_.indextypename).mapValues(_.map(_.indexname))
            val index = indexes.values.toSeq.flatten
              .map(indexname => Index.load(indexname, false).get)
              .sortBy(index => -optimizer.getScore(index, nnq.get)).head

            val entity = Entity.load(entityname).get

            if (optimizer.getScore(index, nnq.get) > optimizer.getScore(entity, nnq.get)) {
              scan = Some(IndexScanExpression(index)(nnq.get, None)(scan)(ac))
            } else {
              scan = Some(SequentialScanExpression(entity)(nnq.get, None)(scan)(ac))
            }

            log.debug("hint-based chose (empirical) " + scan.get.toString)

            return scan
          case SCORED =>
            //TODO: clean code duplication
          log.trace("measurement-based (scored) execution plan hint")
            val optimizer = ac.optimizerRegistry.value.apply("naive").get

            val indexes = SparkStartup.catalogOperator.listIndexes(Some(entityname), Some(nnq.get.attribute)).get.map(Index.load(_)).filter(_.isSuccess).map(_.get).groupBy(_.indextypename).mapValues(_.map(_.indexname))
            val index = indexes.values.toSeq.flatten
              .map(indexname => Index.load(indexname, false).get)
              .sortBy(index => -optimizer.getScore(index, nnq.get)).head

            val entity = Entity.load(entityname).get

            if (optimizer.getScore(index, nnq.get) > optimizer.getScore(entity, nnq.get)) {
              scan = Some(IndexScanExpression(index)(nnq.get, None)(scan)(ac))
            } else {
              scan = Some(SequentialScanExpression(entity)(nnq.get, None)(scan)(ac))
            }

            log.debug("hint-based (scored) chose " + scan.get.toString)

            return scan
          case iqh: IndexQueryHint =>
            log.trace("index execution plan hint")
            val optimizer = ac.optimizerRegistry.value.apply("naive").get

            //index scan
            val indexChoice = SparkStartup.catalogOperator.listIndexes(Some(entityname), Some(nnq.get.attribute), Some(iqh.structureType)).get.map(Index.load(_)).filter(_.isSuccess).map(_.get)

            if (indexChoice.nonEmpty) {
              val sortedIndexChoice = indexChoice
                .filter(nnq.get.isConform(_)) //choose only indexes that are conform to query
                .filterNot(_.isStale) //don't use stale indexes
                .sortBy(index => -optimizer.getScore(index, nnq.get)) //order by weight (highest weight first)

              if (sortedIndexChoice.isEmpty) {
                return None
              }

              scan = Some(IndexScanExpression(sortedIndexChoice.head)(nnq.get, None)(scan)(ac))
              return scan
            } else {
              return scan
            }

          case SEQUENTIAL_QUERY =>
            log.trace("sequential execution plan hint")
            scan = Some(new SequentialScanExpression(entityname)(nnq.get, None)(scan)(ac)) //sequential
            return scan

          case cqh: ComplexQueryHint => {
            log.trace("compound query hint, re-iterate sub-hints")

            //complex query hint
            val chint = cqh.hints
            var i = 0

            while (i < chint.length) {
              val plan = getPlan(entityname, nnq, bq, Seq(chint(i)))(expr)
              if (plan.isDefined) return plan

              i += 1
            }

            return scan
          }
          case _ => scan //default
        }

        j += 1
      }

      scan
    }
  }

}