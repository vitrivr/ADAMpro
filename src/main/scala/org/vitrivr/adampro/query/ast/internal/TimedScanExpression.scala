package org.vitrivr.adampro.query.ast.internal

import org.vitrivr.adampro.data.entity.Entity._
import org.vitrivr.adampro.query.ast.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.query.RankingQuery
import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.query.execution.parallel.{ParallelPathChooser, ParallelQueryHandler}

import scala.concurrent.duration.Duration

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class TimedScanExpression(private val exprs: Seq[QueryExpression], private val timelimit: Duration, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: SharedComponentContext) extends QueryExpression(id) {
  var confidence : Option[Float] = None

  override val info = ExpressionDetails(None, Some("Timed Scan Expression"), id, confidence)
  _children ++= exprs ++ filterExpr.map(Seq(_)).getOrElse(Seq())

  def this(entityname: EntityName, nnq: RankingQuery, pathChooser: ParallelPathChooser, timelimit: Duration, id: Option[String])(filterExpr: Option[QueryExpression])(implicit ac: SharedComponentContext) = {
    this(pathChooser.getPaths(entityname, nnq), timelimit, id)(filterExpr)
  }

  /**
    *
    * @return
    */
  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Option[DataFrame] = {
    log.trace("perform time-limited evaluation")

    ac.sc.setJobGroup(id.getOrElse(""), "timed parallel query", interruptOnCancel = true)

    val prefilter = if (filter.isDefined && filterExpr.isDefined) {
      Some(filter.get.join(filterExpr.get.execute(options)(tracker).get))
    } else if (filter.isDefined) {
      filter
    } else if (filterExpr.isDefined){
      filterExpr.get.execute(options)(tracker)
    } else {
      None
    }

    val res = ParallelQueryHandler.timedParallelQuery(exprs, timelimit, prefilter, options, id)(tracker)

    confidence = Some(res.confidence)
    res.results
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: TimedScanExpression => this.exprs.equals(that.exprs) && this.timelimit.equals(that.timelimit)
      case _ => false
    }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + exprs.hashCode
    result = prime * result + timelimit.hashCode
    result
  }
}
