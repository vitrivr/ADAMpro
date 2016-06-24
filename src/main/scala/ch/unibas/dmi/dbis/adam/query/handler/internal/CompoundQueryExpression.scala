package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryExpression}
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class CompoundQueryExpression(private val expr : QueryExpression, id: Option[String] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Compound Query Expression"), id, None)
  children ++= Seq(expr)

  override protected def run(filter : Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("evaluate compound query")

    ac.sc.setJobGroup(id.getOrElse(""), "compound query", interruptOnCancel = true)

    expr.filter = filter
    expr.evaluate()
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: CompoundQueryExpression => this.expr.equals(that.expr)
      case _ => expr.equals(that)
    }

  override def hashCode(): Int = expr.hashCode
}
