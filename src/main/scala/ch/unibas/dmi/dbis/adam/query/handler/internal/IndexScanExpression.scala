package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.QueryNotConformException
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class IndexScanExpression(index: Index)(nnq: NearestNeighbourQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(implicit ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(Some(index.indextypename.name + " (" + index.indexname + ")"), Some("Index Scan Expression"), id, Some(index.confidence))
  children ++= filterExpr.map(Seq(_)).getOrElse(Seq())

  def this(indexname: IndexName)(nnq: NearestNeighbourQuery, id: Option[String] = None)(expr: Option[QueryExpression] = None)(implicit ac: AdamContext) {
    this(Index.load(indexname).get)(nnq, id)(expr)
  }

  def this(entityname: EntityName, indextypename: IndexTypeName)(nnq: NearestNeighbourQuery, id: Option[String] = None)(expr: Option[QueryExpression] = None)(implicit ac: AdamContext) {
    this(
      Entity.load(entityname).get.indexes
        .filter(_.isSuccess)
        .map(_.get)
        .filter(_.isQueryConform(nnq)) //choose only indexes that are conform to query
        .sortBy(-_.weight)
        .head
    )(nnq, id)(expr)
  }

  override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("performing index scan operation")

    if (!index.isQueryConform(nnq)) {
      throw QueryNotConformException()
    }

    if (!index.entity.get.isQueryConform(nnq)) {
      throw QueryNotConformException()
    }

    val prefilter = if (filter.isDefined && filterExpr.isDefined) {
      val pk = index.entity.get.pk
      Some(filter.get.select(pk.name).join(filterExpr.get.evaluate().get, pk.name))
    } else if (filter.isDefined) {
      filter
    } else if (filterExpr.isDefined){
      filterExpr.get.evaluate()
    } else {
      None
    }

    Some(IndexScanExpression.scan(index)(prefilter, nnq, id))
  }

  override def prepareTree() : QueryExpression = {
    super.prepareTree()
    if(!nnq.indexOnly){
      return SequentialScanExpression(index.entityname)(nnq, id)(Some(this)) //add sequential scan if not only scanning index
    } else {
      this
    }
  }
}

object IndexScanExpression extends Logging {
  /**
    * Performs a index-based query.
    *
    * @param index
    * @param nnq
    * @param id
    * @return
    */
  def scan(index: Index)(filter: Option[DataFrame], nnq: NearestNeighbourQuery, id: Option[String] = None)(implicit ac: AdamContext): DataFrame = {
    index.scan(nnq, filter)
  }
}