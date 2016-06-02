package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.handler.internal.BooleanFilterExpression.BooleanFilterScanExpression
import ch.unibas.dmi.dbis.adam.query.handler.internal.QueryHints.{ComplexQueryHint, IndexQueryHint, QueryHint, SEQUENTIAL_QUERY}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class HintBasedScanExpression(entityname: EntityName, nnq: Option[NearestNeighbourQuery], bq: Option[BooleanQuery], hints: Seq[QueryHint], id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(implicit ac: AdamContext) extends QueryExpression(id) {
  val expr = HintBasedScanExpression.startPlanSearch(entityname, nnq, bq, hints)(filterExpr)
  override val info = ExpressionDetails(expr.info.source, Some("Hint-Based Expression: " + expr.info.scantype), id,  expr.info.confidence)
  children ++= Seq(expr) ++ filterExpr.map(Seq(_)).getOrElse(Seq())

  override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("evaluate hint-based expression, scanning " + expr.info.scantype)
    expr.filter = filter
    expr.evaluate()
  }
}

object HintBasedScanExpression extends Logging {

  def startPlanSearch(entityname: EntityName, nnq: Option[NearestNeighbourQuery], bq: Option[BooleanQuery], hints: Seq[QueryHint])(expr: Option[QueryExpression] = None)(implicit ac: AdamContext): QueryExpression = {
    val indexes: Map[IndexTypeName, Seq[IndexName]] = CatalogOperator.listIndexes(entityname).groupBy(_._2).mapValues(_.map(_._1))
    var plan = getPlan(entityname, indexes, nnq, bq, hints)(expr)

    if (plan.isEmpty) {
      log.trace("no query plan chosen, go to fallback")
      plan = getPlan(entityname, indexes, nnq, bq, Seq(QueryHints.FALLBACK_HINTS))(expr)
    }

    if (plan.isDefined) {
      log.debug("using plan: " + plan.get.getClass.getName)
    } else {
      throw new GeneralAdamException("no execution plan set up")
    }

    plan.get
  }

  /**
    * Chooses the query plan to use based on the given hint, the available indexes, etc.
    *
    * @param entityname name of entity
    * @param indexes    possible indexes
    * @param nnq        nearest neighbour query
    * @param bq         boolean query
    * @param hints      query hints
    * @param expr       filter expression
    * @return
    */
  private def getPlan(entityname: EntityName, indexes: Map[IndexTypeName, Seq[IndexName]], nnq: Option[NearestNeighbourQuery], bq: Option[BooleanQuery], hints: Seq[QueryHint])(expr: Option[QueryExpression] = None)(implicit ac: AdamContext): Option[QueryExpression] = {


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
      scan = Some(BooleanFilterScanExpression(entityname)(bq.get)(expr))
    }

    if (nnq.isEmpty) {
      return scan
    } else {
      var j = 0
      while (j < hints.length) {
        hints(j) match {
          case iqh: IndexQueryHint => {
            log.trace("index execution plan hint")
            //index scan
            val indexChoice = indexes.get(iqh.structureType)

            if (indexChoice.isDefined) {
              val indexes = indexChoice.get
                .map(indexname => Index.load(indexname, false).get)
                .filter(_.isQueryConform(nnq.get)) //choose only indexes that are conform to query
                .filterNot(_.isStale) //don't use stale indexes
                .sortBy(-_.weight) //order by weight (highest weight first)

              if (indexes.isEmpty) {
                return None
              }

              scan = Some(IndexScanExpression(indexes.head)(nnq.get)(scan))
              return scan
            } else {
              return scan
            }
          }
          case SEQUENTIAL_QUERY =>
            log.trace("sequential execution plan hint")
            scan = Some(SequentialScanExpression(entityname)(nnq.get)(scan)) //sequential
            return scan

          case cqh: ComplexQueryHint => {
            log.trace("compound query hint, re-iterate sub-hints")

            //complex query hint
            val chint = cqh.hints
            var i = 0

            while (i < chint.length) {
              val plan = getPlan(entityname, indexes, nnq, bq, Seq(chint(i)))(expr)
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