package org.vitrivr.adampro.query.handler.internal

import org.vitrivr.adampro.config.FieldNames
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.handler.generic.{QueryEvaluationOptions, ExpressionDetails, QueryExpression}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.apache.http.annotation.Experimental
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  *
  * Scans multiple indices and combines unprecise results.
  */
@Experimental
case class StochasticIndexQueryExpression(private val exprs: Seq[IndexScanExpression])(nnq: NearestNeighbourQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Compound Query Index Scan Expression"), id, None)
  _children ++= filterExpr.map(Seq(_)).getOrElse(Seq())
  //expres is not added to children as they would be "prepared" for querying, resulting possibly in a sequential scan
  var confidence: Option[Float] = None

  //only work on one entity
  assert(exprs.map(_.index.entityname).distinct.length == 1)
  //use multiple indices
  assert(exprs.length >= 2)

  val entity = exprs.head.index.entity.get

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("evaluate compound query index scan")

    ac.sc.setJobGroup(id.getOrElse(""), "compound query index scan", interruptOnCancel = true)

    exprs.map(_.filter = filter)
    val results = exprs.map(expr => {
      //make sure that index only is queried and not a sequential scan too!
      expr.evaluate(options)
    })

    var result = results.filter(_.isDefined).map(_.get).reduce[DataFrame] { case (a, b) => a.select(entity.pk.name, FieldNames.distanceColumnName).unionAll(b.select(entity.pk.name, FieldNames.distanceColumnName)) }
      .groupBy(entity.pk.name).agg(count("*").alias("adampro_result_appears_in_n_joins"))
      .withColumn(FieldNames.distanceColumnName, distUDF(col("adampro_result_appears_in_n_joins")))

    result = result.select(entity.pk.name, FieldNames.distanceColumnName)

    if (options.isDefined && options.get.storeSourceProvenance) {
      result = result.withColumn(FieldNames.sourceColumnName, lit(info.scantype.getOrElse("undefined")))
    }

    Some(result)
  }

  val distUDF = udf((count: Int) => {
    //TODO: possibly use indexDistance for more precise evaluation of distance
    1 - (count / exprs.length).toFloat
  })


  override def prepareTree(): QueryExpression = {
    super.prepareTree()
    if (!nnq.indexOnly) {
      new SequentialScanExpression(entity)(nnq, id)(Some(this)) //add sequential scan if not only scanning index
    } else {
      this
    }
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: StochasticIndexQueryExpression => this.exprs.equals(that.exprs)
      case _ => exprs.equals(that)
    }

  override def hashCode(): Int = exprs.hashCode
}
