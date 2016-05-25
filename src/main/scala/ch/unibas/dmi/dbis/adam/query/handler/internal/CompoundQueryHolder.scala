package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.{RunDetails, QueryExpression}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class CompoundQueryHolder(expr: QueryExpression, id: Option[String] = None) extends QueryExpression(id) {
  override protected def run(filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
    expr.evaluate(filter)
  }

  override def getRunDetails(info: ListBuffer[RunDetails]) = {
    super.getRunDetails(info)
    expr.getRunDetails(info)
  }
}
