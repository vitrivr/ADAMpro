package org.vitrivr.adampro.query.handler.internal

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.vitrivr.adampro.config.FieldNames
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity._
import org.vitrivr.adampro.exception.QueryNotConformException
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.index.Index._
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.handler.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.vitrivr.adampro.utils.Logging

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

  def this(indexname: IndexName)(nnq: NearestNeighbourQuery, id: Option[String])(filterExpr: Option[QueryExpression])(implicit ac: AdamContext) {
    this(Index.load(indexname).get)(nnq, id)(filterExpr)
  }

  def this(entityname: EntityName, indextypename: IndexTypeName)(nnq: NearestNeighbourQuery, id: Option[String])(filterExpr: Option[QueryExpression])(implicit ac: AdamContext) {
    this(
      Entity.load(entityname).get.indexes
        .filter(_.isSuccess)
        .map(_.get)
        .filter(nnq.isConform(_)) //choose only indexes that are conform to query
        .sortBy(index => - ac.optimizerRegistry.value.apply("naive").get.getScore(index, nnq))
        .head
    )(nnq, id)(filterExpr)
  }

  override protected def run(options: Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("performing index scan operation")

    ac.sc.setLocalProperty("spark.scheduler.pool", "index")
    ac.sc.setJobGroup(id.getOrElse(""), "index scan: " + index.indextypename.name, interruptOnCancel = true)

    log.trace(QUERY_MARKER, "checking conformity of index scan")

    if (!nnq.isConform(index)) {
      throw QueryNotConformException("query is not conform to index")
    }

    log.trace(QUERY_MARKER, "preparing filtering ids")

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

    log.trace(QUERY_MARKER, "starting index scan")

    var res = IndexScanExpression.scan(index)(prefilter, nnq, id)

    log.trace(QUERY_MARKER, "finished index scan")

    if (options.isDefined && options.get.storeSourceProvenance) {
      res = res.withColumn(FieldNames.sourceColumnName, lit(sourceDescription))
    }

    Some(res)
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