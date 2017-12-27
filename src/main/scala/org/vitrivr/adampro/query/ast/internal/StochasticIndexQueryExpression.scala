package org.vitrivr.adampro.query.ast.internal

import org.apache
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.query.ast.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.query.RankingQuery
import org.apache.http.annotation.Experimental
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.vitrivr.adampro.data.datatypes.TupleID
import org.vitrivr.adampro.data.datatypes.TupleID.TupleID
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.tracker.QueryTracker

import scala.util.Success

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  *
  * Scans multiple indices and combines unprecise results.
  */
@Experimental
case class StochasticIndexQueryExpression(private val exprs: Seq[IndexScanExpression])(nnq: RankingQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: SharedComponentContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Compound Query Index Scan Expression"), id, None)
  _children ++= filterExpr.map(Seq(_)).getOrElse(Seq())
  //expres is not added to children as they would be "prepared" for querying, resulting possibly in a sequential scan
  var confidence: Option[Float] = None

  //only work on one entity
  assert(exprs.map(_.index.entityname).distinct.length == 1)
  //use multiple indices
  assert(exprs.length >= 2)

  val entity = exprs.head.index.entity.get

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Option[DataFrame] = {
    log.trace("evaluate compound query index scan")

    ac.sc.setJobGroup(id.getOrElse(""), "compound query index scan", interruptOnCancel = true)

    exprs.map(_.filter = filter)
    val subresults = exprs.map(expr => {
      //make sure that index only is queried and not a sequential scan too!
      expr.execute(options)(tracker)
    }).filter(_.isDefined).map(_.get.select(entity.pk.name))

    var result = ac.spark.createDataFrame(ac.sc.emptyRDD[Row], StructType(StructField(entity.pk.name, TupleID.SparkTupleID) :: Nil))

    for (subresult <- subresults) {
      result = result.union(subresult)
    }

    val nsubres = subresults.length.toDouble
    val groupingExpr = col(entity.pk.name) % ac.config.defaultNumberOfPartitions as "group"

    result = result.repartition(numPartitions = ac.config.defaultNumberOfPartitions, groupingExpr)
      .groupBy(entity.pk.name).agg((lit(1.0) - (count(col(entity.pk.name)) / nsubres)) as AttributeNames.distanceColumnName)
      .orderBy(AttributeNames.distanceColumnName)
      .limit(math.min(50 * nnq.k, 5000))

    if (options.isDefined && options.get.storeSourceProvenance) {
      result = result.withColumn(AttributeNames.sourceColumnName, lit(info.scantype.getOrElse("undefined")))
    }

    Some(result)
  }


  override def rewrite(silent : Boolean = false): QueryExpression = {
    super.rewrite(silent)
    if (!nnq.indexOnly) {
      val expr = new SequentialScanExpression(entity)(nnq, id)(Some(this)) //add sequential scan if not only scanning index
      expr.prepared = true
      expr
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
