package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.progressive.{ProgressivePathChooser, ProgressiveQueryHandler}
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class TimedScanExpression(private val exprs: Seq[QueryExpression], private val timelimit: Duration, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Timed Scan Expression"), id, confidence)
  children ++= exprs ++ filterExpr.map(Seq(_)).getOrElse(Seq())
  var confidence : Option[Float] = None

  def this(entityname: EntityName, nnq: NearestNeighbourQuery, pathChooser: ProgressivePathChooser, timelimit: Duration, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(implicit ac: AdamContext) = {
    this(pathChooser.getPaths(entityname, nnq), timelimit, id)(filterExpr)
  }

  /**
    *
    * @return
    */
  override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("perform time-limited evaluation")

    ac.sc.setJobGroup(id.getOrElse(""), "timed progressive query", interruptOnCancel = true)

    val prefilter = if (filter.isDefined && filterExpr.isDefined) {
      Some(filter.get.join(filterExpr.get.evaluate().get))
    } else if (filter.isDefined) {
      filter
    } else if (filterExpr.isDefined){
      filterExpr.get.evaluate()
    } else {
      None
    }

    val res = ProgressiveQueryHandler.timedProgressiveQuery(exprs, timelimit, prefilter, id)

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
