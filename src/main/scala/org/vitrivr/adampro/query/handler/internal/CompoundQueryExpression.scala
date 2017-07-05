package org.vitrivr.adampro.query.handler.internal

import org.vitrivr.adampro.main.SharedComponentContext
import org.vitrivr.adampro.query.handler.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.helpers.tracker.OperationTracker

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class CompoundQueryExpression(private val expr : QueryExpression, id: Option[String] = None)(@transient implicit val ac: SharedComponentContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Compound Query Expression"), id, None)
  _children ++= Seq(expr)

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : OperationTracker)(implicit ac: SharedComponentContext): Option[DataFrame] = {
    log.debug("evaluate compound query")

    ac.sc.setJobGroup(id.getOrElse(""), "compound query", interruptOnCancel = true)

    expr.filter = filter
    expr.evaluate(options)(tracker)
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: CompoundQueryExpression => this.expr.equals(that.expr)
      case _ => expr.equals(that)
    }

  override def hashCode(): Int = expr.hashCode
}
