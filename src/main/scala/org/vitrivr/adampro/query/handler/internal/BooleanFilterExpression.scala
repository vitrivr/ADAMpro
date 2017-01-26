package org.vitrivr.adampro.query.handler.internal

import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity._
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.handler.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.query.BooleanQuery
import org.vitrivr.adampro.utils.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
object BooleanFilterExpression extends Logging {

  /**
    *
    * @param entity entity
    * @param bq     boolean query
    */
  case class BooleanFilterScanExpression(private val entity: Entity)(private val bq: BooleanQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
    override val info = ExpressionDetails(Some(entity.entityname), Some("Table Boolean-Scan Expression"), id, None)
    _children ++= filterExpr.map(Seq(_)).getOrElse(Seq())

    def this(entityname: EntityName)(bq: BooleanQuery, id: Option[String])(filterExpr: Option[QueryExpression])(implicit ac: AdamContext) {
      this(Entity.load(entityname).get)(bq, id)(filterExpr)(ac)
    }

    override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
      log.debug("run boolean filter operation on entity")

      ac.sc.setJobGroup(id.getOrElse(""), "boolean filter scan", interruptOnCancel = true)

      var df =  entity.getData(predicates = bq.where)

      var ids = mutable.Set[Any]()

      if (filter.isDefined) {
        ids ++= filter.get.select(entity.pk.name).collect().map(_.getAs[Any](entity.pk.name))
      }

      if (filterExpr.isDefined) {
        filterExpr.get.filter = filter
        ids ++= filterExpr.get.evaluate(options).get.select(entity.pk.name).collect().map(_.getAs[Any](entity.pk.name))
      }

      if (ids.nonEmpty) {
        val idsbc = ac.sc.broadcast(ids)
        df = df.map(d => {
          val rdd = d.rdd.filter(x => idsbc.value.contains(x.getAs[Any](entity.pk.name)))
          ac.sqlContext.createDataFrame(rdd, d.schema)
        })
      }

      df.map(BooleanFilterExpression.filter(_, bq))
    }

    override def equals(that: Any): Boolean =
      that match {
        case that: BooleanFilterScanExpression => this.entity.equals(that.entity) && this.bq.equals(that.bq)
        case _ => false
      }

    override def hashCode(): Int = {
      val prime = 31
      var result = 1
      result = prime * result + entity.hashCode
      result = prime * result + bq.hashCode
      result
    }
  }

  /**
    *
    * @param bq boolean query
    */
  case class BooleanFilterAdHocExpression(expr: QueryExpression, bq: BooleanQuery, id: Option[String] = None)(implicit ac: AdamContext) extends QueryExpression(id) {
    override val info = ExpressionDetails(None, Some("Ad-Hoc Boolean-Scan Expression"), id, None)
    _children ++= Seq(expr)

    override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
      log.debug("run boolean filter operation on data")

      ac.sc.setJobGroup(id.getOrElse(""), "boolean filter scan", interruptOnCancel = true)

      var result = expr.evaluate(options)

      if (filter.isDefined) {
        result = result.map(_.join(filter.get))
      }

      if (result.isDefined && options.isDefined && options.get.storeSourceProvenance) {
        result = Some(result.get.withColumn(AttributeNames.sourceColumnName, lit(info.scantype.getOrElse("undefined"))))
      }

      result.map(BooleanFilterExpression.filter(_, bq))
    }

    override def equals(that: Any): Boolean =
      that match {
        case that: BooleanFilterAdHocExpression => this.expr.equals(that.expr) && this.bq.equals(that.bq)
        case _ => false
      }

    override def hashCode(): Int = {
      val prime = 31
      var result = 1
      result = prime * result + expr.hashCode
      result = prime * result + bq.hashCode
      result
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

    val sqlString = bq.where.map(_.sqlString).mkString(" AND ")
    log.debug("query metadata using where clause: " + sqlString)
    data = data.filter(sqlString)

    data
  }
}