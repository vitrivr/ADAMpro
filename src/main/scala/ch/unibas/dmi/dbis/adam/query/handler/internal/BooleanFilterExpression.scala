package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.query.BooleanQuery
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
object BooleanFilterExpression extends Logging {

  /**
    *
    * @param entityname name of entity
    * @param bq boolean query
    */
  case class BooleanFilterScanExpression(entityname: EntityName)(bq: BooleanQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(implicit ac: AdamContext) extends QueryExpression(id) {
    override val info = ExpressionDetails(Some(entityname), Some("Table Boolean-Scan Expression"), id, None)
    children ++= filterExpr.map(Seq(_)).getOrElse(Seq())

    override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
      log.debug("run boolean filter operation on entity")

      val entity = Entity.load(entityname).get
      var df = entity.data

      if (filter.isDefined) {
        df = df.map(_.join(filter.get, entity.pk.name))
      }

      if (filterExpr.isDefined) {
        filterExpr.get.filter = filter
        df = df.map(_.join(filterExpr.get.evaluate().get, entity.pk.name))
      }

      df.map(BooleanFilterExpression.filter(_, bq))
    }

  }

  /**
    *
    * @param bq boolean query
    */
  case class BooleanFilterAdHocExpression(expr: QueryExpression, bq: BooleanQuery, id: Option[String] = None)(implicit ac: AdamContext) extends QueryExpression(id) {
    override val info = ExpressionDetails(None, Some("Ad-Hoc Boolean-Scan Expression"), id, None)
    children ++= Seq(expr)

    override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
      log.debug("run boolean filter operation on data")

      var df = expr.evaluate()

      if (filter.isDefined) {
        df = df.map(_.join(filter.get))
      }

      df.map(BooleanFilterExpression.filter(_, bq))
    }
  }


  /**
    *
    * @param df data frame
    * @param bq boolean query
    * @return
    */
  def filter(df: DataFrame, bq: BooleanQuery)(implicit ac: AdamContext): DataFrame = {
    log.debug("filter using boolean query filter")
    var data = df

    if (bq.join.isDefined) {
      log.debug("join tables to results")
      val joins = bq.join.get

      for (i <- joins.indices) {
        val join = joins(i)
        log.debug("join " + join._1 + " on " + join._2.mkString("(", ", ", ")"))
        val newDF = SparkStartup.metadataStorage.read(join._1)

        if (newDF.isSuccess) {
          data = data.join(newDF.get, join._2)
        }
      }
    }

    if (bq.where.isDefined) {
      val where = bq.buildWhereClause()
      log.debug("query metadata using where clause: " + where)
      data = data.filter(where)
    }

    data
  }
}