package org.vitrivr.adampro.query.ast.internal

import org.vitrivr.adampro.data.entity.Entity._
import org.vitrivr.adampro.utils.exception.GeneralAdamException
import org.vitrivr.adampro.data.index.Index
import org.vitrivr.adampro.query.ast.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.ast.internal.BooleanFilterExpression.BooleanFilterScanExpression
import org.vitrivr.adampro.query.query.{FilteringQuery, QueryHints, RankingQuery}
import org.vitrivr.adampro.query.planner.{QueryPlannerOp, PlannerRegistry}
import org.vitrivr.adampro.utils.Logging
import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.query.QueryHints._
import org.vitrivr.adampro.query.tracker.QueryTracker

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class HintBasedScanExpression(private val entityname: EntityName, private val nnq: Option[RankingQuery], private val bq: Option[FilteringQuery], private val hints: Seq[QueryHint], private val withFallback: Boolean = true, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: SharedComponentContext) extends QueryExpression(id) {
  var expr = HintBasedScanExpression.startPlanSearch(entityname, nnq, bq, hints, withFallback)(filterExpr)
  override val info = ExpressionDetails(expr.info.source, Some("Hint-Based Expression: " + expr.info.scantype), id, expr.info.confidence)
  _children ++= Seq(expr) ++ filterExpr.map(Seq(_)).getOrElse(Seq())

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Option[DataFrame] = {
    log.trace("evaluate hint-based expression, scanning " + expr.info.scantype)
    expr.filter = filter
    expr.execute(options)(tracker)
  }

  override def rewrite(silent : Boolean = false): QueryExpression = {
    super.rewrite(silent)
    expr = expr.rewrite(silent = true) //expr needs to be replaced
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

  def startPlanSearch(entityname: EntityName, nnq: Option[RankingQuery], bq: Option[FilteringQuery], hints: Seq[QueryHint], withFallback: Boolean = true)(expr: Option[QueryExpression] = None)(implicit ac: SharedComponentContext): QueryExpression = {
    log.trace(QUERY_MARKER, "start hint based plan search")
    var plan = getPlan(entityname, nnq, bq, hints)(expr)

    if (plan.isEmpty) {
      if (withFallback) {
        log.warn("no query plan chosen, go to fallback")
        plan = getPlan(entityname, nnq, bq, Seq(QueryHints.FALLBACK_HINTS))(expr)
      } else {
        throw new GeneralAdamException("using hints no execution plan could be found, using empty plan")
      }
    }

    log.trace("using plan: " + plan.get.getClass.getName)
    log.trace(QUERY_MARKER, "end hint based plan search")

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
  private def getPlan(entityname: EntityName, nnq: Option[RankingQuery], bq: Option[FilteringQuery], hints: Seq[QueryHint])(expr: Option[QueryExpression] = None)(implicit ac: SharedComponentContext): Option[QueryExpression] = {
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
          case eqh : EmpiricalQueryHint =>
            log.trace("measurement-based (empirical) execution plan hint")

            scan = Some(QueryPlannerOp.getOptimalScan(eqh.optimizerName, entityname, nnq.get)(scan)(ac))

            log.trace("hint-based chose (empirical) " + scan.get.toString)

            return scan

          case iqh: IndexQueryHint =>
            log.trace("index execution plan hint")
            val optimizer = PlannerRegistry.apply("naive").get

            //index scan
            val indexChoice = ac.catalogManager.listIndexes(Some(entityname), Some(nnq.get.attribute), Some(iqh.structureType)).get.map(Index.load(_)).filter(_.isSuccess).map(_.get)

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