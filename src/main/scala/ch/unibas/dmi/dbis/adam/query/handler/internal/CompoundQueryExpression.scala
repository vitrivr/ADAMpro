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
case class CompoundQueryExpression(expr : QueryExpression, id: Option[String] = None)(implicit ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Compound Query Expression"), id, None)
  children ++= Seq(expr)

  override protected def run(filter : Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    expr.evaluate()
  }
}
