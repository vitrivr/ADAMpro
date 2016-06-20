package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.handler.internal.AggregationExpression.EmptyExpression
import ch.unibas.dmi.dbis.adam.query.handler.internal.BooleanFilterExpression.BooleanFilterScanExpression
import ch.unibas.dmi.dbis.adam.query.handler.internal.QueryHints._
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class HintBasedScanExpression(private val entityname: EntityName, private val nnq: Option[NearestNeighbourQuery], private val bq: Option[BooleanQuery], private val hints: Seq[QueryHint], private val withFallback: Boolean = true, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  val expr = HintBasedScanExpression.startPlanSearch(entityname, nnq, bq, hints, withFallback)(filterExpr)
  override val info = ExpressionDetails(expr.info.source, Some("Hint-Based Expression: " + expr.info.scantype), id, expr.info.confidence)
  children ++= Seq(expr) ++ filterExpr.map(Seq(_)).getOrElse(Seq())

  override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("evaluate hint-based expression, scanning " + expr.info.scantype)
    expr.filter = filter
    expr.evaluate()
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
    val indexes: Map[IndexTypeName, Seq[IndexName]] = CatalogOperator.listIndexes(entityname).groupBy(_._2).mapValues(_.map(_._1))
    var plan = getPlan(entityname, indexes, nnq, bq, hints)(expr)

    if (plan.isEmpty) {
      if (withFallback) {
        log.trace("no query plan chosen, go to fallback")
        plan = getPlan(entityname, indexes, nnq, bq, Seq(QueryHints.FALLBACK_HINTS))(expr)
      } else {
        log.warn("using hints no execution plan could be found, using empty plan")
        plan = Some(EmptyExpression())
      }
    }

    log.debug("using plan: " + plan.get.getClass.getName)

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
      scan = Some(new BooleanFilterScanExpression(entityname)(bq.get)(expr))
    }

    if (nnq.isEmpty) {
      return scan
    } else {
      var j = 0
      while (j < hints.length) {
        hints(j) match {
          case PREDICTIVE =>
            log.trace("measurement-based execution plan hint")
            val index = indexes.values.toSeq.flatten
              .map(indexname => Index.load(indexname, false).get)
              .sortBy(-_.scanweight).head

            val entity = Entity.load(entityname).get

            if (index.scanweight > entity.scanweight(nnq.get.column)) {
              scan = Some(IndexScanExpression(index)(nnq.get)(scan))
            } else {
              scan = Some(SequentialScanExpression(entity)(nnq.get)(scan))
            }

            return scan
          case iqh: IndexQueryHint =>
            log.trace("index execution plan hint")
            //index scan
            val indexChoice = indexes.get(iqh.structureType)

            if (indexChoice.isDefined) {
              val indexes = indexChoice.get
                .map(indexname => Index.load(indexname, false).get)
                .filter(_.isQueryConform(nnq.get)) //choose only indexes that are conform to query
                .filterNot(_.isStale) //don't use stale indexes
                .sortBy(-_.scanweight) //order by weight (highest weight first)

              if (indexes.isEmpty) {
                return None
              }

              scan = Some(IndexScanExpression(indexes.head)(nnq.get)(scan))
              return scan
            } else {
              return scan
            }

          case SEQUENTIAL_QUERY =>
            log.trace("sequential execution plan hint")
            scan = Some(new SequentialScanExpression(entityname)(nnq.get)(scan)) //sequential
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