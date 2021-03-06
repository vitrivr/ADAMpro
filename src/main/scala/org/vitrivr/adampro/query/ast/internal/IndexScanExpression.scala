package org.vitrivr.adampro.query.ast.internal

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.data.entity.Entity
import org.vitrivr.adampro.data.entity.Entity._
import org.vitrivr.adampro.utils.exception.QueryNotConformException
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.data.index.Index
import org.vitrivr.adampro.data.index.Index._
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.ast.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.planner.PlannerRegistry
import org.vitrivr.adampro.query.query.RankingQuery
import org.vitrivr.adampro.utils.Logging

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class IndexScanExpression(val index: Index)(val nnq: RankingQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: SharedComponentContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(Some(index.indexname), Some("Index Scan Expression"), id, Some(index.confidence), Map("indextype" -> index.indextypename.name))
  val sourceDescription = {
    if (filterExpr.isDefined) {
      filterExpr.get.info.scantype.getOrElse("undefined") + "->" + info.scantype.getOrElse("undefined")
    } else {
      info.scantype.getOrElse("undefined")
    }
  }

  _children ++= filterExpr.map(Seq(_)).getOrElse(Seq())

  def this(indexname: IndexName)(nnq: RankingQuery, id: Option[String])(filterExpr: Option[QueryExpression])(implicit ac: SharedComponentContext) {
    this(Index.load(indexname).get)(nnq, id)(filterExpr)
  }

  def this(entityname: EntityName, indextypename: IndexTypeName)(nnq: RankingQuery, id: Option[String])(filterExpr: Option[QueryExpression])(implicit ac: SharedComponentContext) {
    this(
      Entity.load(entityname).get.indexes
        .filter(_.isSuccess)
        .map(_.get)
        .filter(nnq.isConform(_)) //choose only indexes that are conform to query
        .sortBy(index => - PlannerRegistry("naive").get.getScore(index, nnq))
        .head
    )(nnq, id)(filterExpr)
  }

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Option[DataFrame] = {
    log.trace("performing index scan operation")

    ac.sc.setLocalProperty("spark.scheduler.pool", "index")
    ac.sc.setJobGroup(id.getOrElse(""), "index scan: " + index.indextypename.name, interruptOnCancel = true)

    log.trace(QUERY_MARKER, "checking conformity of index scan")

    if (!nnq.isConform(index)) {
      throw QueryNotConformException("query is not conform to index")
    }

    log.trace(QUERY_MARKER, "preparing filtering ids")

    val prefilter = if (filter.isDefined && filterExpr.isDefined) {
      val pk = index.entity.get.pk
      Some(filter.get.select(pk.name).join(filterExpr.get.execute(options)(tracker).get, pk.name))
    } else if (filter.isDefined) {
      filter
    } else if (filterExpr.isDefined) {
      filterExpr.get.execute(options)(tracker)
    } else {
      None
    }

    log.trace(QUERY_MARKER, "starting index scan")

    var res = IndexScanExpression.scan(index)(prefilter, nnq, id)(tracker)

    log.trace(QUERY_MARKER, "finished index scan")

    if (options.isDefined && options.get.storeSourceProvenance) {
      res = res.withColumn(AttributeNames.sourceColumnName, lit(sourceDescription))
    }

    Some(res)
  }

  override def rewrite(silent : Boolean = false): QueryExpression = {
    super.rewrite(silent)
    if (!nnq.indexOnly) {
      val expr = new SequentialScanExpression(index.entityname)(nnq, id)(Some(this)) //add sequential scan if not only scanning index
      expr.prepared = true
      expr
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
  def scan(index: Index)(filter: Option[DataFrame], nnq: RankingQuery, id: Option[String] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): DataFrame = {
    index.scan(nnq, filter)(tracker)
  }
}