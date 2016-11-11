package org.vitrivr.adampro.query.handler.internal

import org.vitrivr.adampro.config.FieldNames
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity._
import org.vitrivr.adampro.exception.QueryNotConformException
import org.vitrivr.adampro.helpers.benchmark.ScanWeightCatalogOperator
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.index.Index._
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.handler.generic.{QueryEvaluationOptions, ExpressionDetails, QueryExpression}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.vitrivr.adampro.utils.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class IndexScanExpression(val index: Index)(val nnq: NearestNeighbourQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(Some(index.indexname), Some("Index Scan Expression"), id, Some(index.confidence), Map("indextype" -> index.indextypename.name))
  val sourceDescription = {
    if (filterExpr.isDefined) {
      filterExpr.get.info.scantype.getOrElse("undefined") + "->" + info.scantype.getOrElse("undefined")
    } else {
      info.scantype.getOrElse("undefined")
    }
  }

  _children ++= filterExpr.map(Seq(_)).getOrElse(Seq())

  def this(indexname: IndexName)(nnq: NearestNeighbourQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(implicit ac: AdamContext) {
    this(Index.load(indexname).get)(nnq, id)(filterExpr)
  }

  def this(entityname: EntityName, indextypename: IndexTypeName)(nnq: NearestNeighbourQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(implicit ac: AdamContext) {
    this(
      Entity.load(entityname).get.indexes
        .filter(_.isSuccess)
        .map(_.get)
        .filter(nnq.isConform(_)) //choose only indexes that are conform to query
        .sortBy(index => -ScanWeightCatalogOperator(index))
        .head
    )(nnq, id)(filterExpr)
  }

  override protected def run(options: Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("performing index scan operation")

    ac.sc.setLocalProperty("spark.scheduler.pool", "index")
    ac.sc.setJobGroup(id.getOrElse(""), "index scan: " + index.indextypename.name, interruptOnCancel = true)

    if (!(nnq.isConform(index) && nnq.isConform(index.entity.get))) {
      throw QueryNotConformException()
    }

    val prefilter = if (filter.isDefined && filterExpr.isDefined) {
      val pk = index.entity.get.pk
      Some(filter.get.select(pk.name).join(filterExpr.get.evaluate(options).get, pk.name))
    } else if (filter.isDefined) {
      filter
    } else if (filterExpr.isDefined) {
      filterExpr.get.evaluate(options)
    } else {
      None
    }

    var result = IndexScanExpression.scan(index)(prefilter, nnq, id)

    if (options.isDefined && options.get.storeSourceProvenance) {
      result = result.withColumn(FieldNames.sourceColumnName, lit(sourceDescription))
    }

    Some(result)
  }

  override def prepareTree(): QueryExpression = {
    super.prepareTree()
    if (!nnq.indexOnly) {
      new SequentialScanExpression(index.entityname)(nnq, id)(Some(this)) //add sequential scan if not only scanning index
    } else {
      this
    }
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: IndexScanExpression => this.index.equals(that.index) && this.nnq.equals(that.nnq)
      case _ => false
    }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + index.hashCode
    result = prime * result + nnq.hashCode
    result
  }
}

object IndexScanExpression extends Logging {
  /**
    * Performs a index-based query.
    *
    * @param index index
    * @param nnq   information on nearest neighbour query
    * @param id    query id
    * @return
    */
  def scan(index: Index)(filter: Option[DataFrame], nnq: NearestNeighbourQuery, id: Option[String] = None)(implicit ac: AdamContext): DataFrame = {
    index.scan(nnq, filter)
  }
}