package org.vitrivr.adampro.query.handler.internal

import org.vitrivr.adampro.entity.Entity._
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.handler.generic.{QueryEvaluationOptions, ExpressionDetails, QueryExpression}
import org.vitrivr.adampro.query.progressive.{ProgressivePathChooser, ProgressiveQueryHandler}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class TimedScanExpression(private val exprs: Seq[QueryExpression], private val timelimit: Duration, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  var confidence : Option[Float] = None

  override val info = ExpressionDetails(None, Some("Timed Scan Expression"), id, confidence)
  _children ++= exprs ++ filterExpr.map(Seq(_)).getOrElse(Seq())

  def this(entityname: EntityName, nnq: NearestNeighbourQuery, pathChooser: ProgressivePathChooser, timelimit: Duration, id: Option[String])(filterExpr: Option[QueryExpression])(implicit ac: AdamContext) = {
    this(pathChooser.getPaths(entityname, nnq), timelimit, id)(filterExpr)
  }

  /**
    *
    * @return
    */
  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame])(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("perform time-limited evaluation")

    ac.sc.setJobGroup(id.getOrElse(""), "timed progressive query", interruptOnCancel = true)

    val prefilter = if (filter.isDefined && filterExpr.isDefined) {
      Some(filter.get.join(filterExpr.get.evaluate(options).get))
    } else if (filter.isDefined) {
      filter
    } else if (filterExpr.isDefined){
      filterExpr.get.evaluate(options)
    } else {
      None
    }

    val res = ProgressiveQueryHandler.timedProgressiveQuery(exprs, timelimit, prefilter, options, id)

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
