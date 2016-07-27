package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.QueryNotConformException
import ch.unibas.dmi.dbis.adam.helpers.scanweight.ScanWeightInspector
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{QueryEvaluationOptions, ExpressionDetails, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class IndexScanExpression(private[handler] val index: Index)(private val nnq: NearestNeighbourQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(Some(index.indextypename.name + " (" + index.indexname + ")"), Some("Index Scan Expression"), id, Some(index.confidence))
  val sourceDescription = {
    if (filterExpr.isDefined) {
      filterExpr.get.info.scantype.getOrElse("undefined") + "->" + info.scantype.getOrElse("undefined")
    } else {
      info.scantype.getOrElse("undefined")
    }
  }

  children ++= filterExpr.map(Seq(_)).getOrElse(Seq())

  def this(indexname: IndexName)(nnq: NearestNeighbourQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(implicit ac: AdamContext) {
    this(Index.load(indexname).get)(nnq, id)(filterExpr)
  }

  def this(entityname: EntityName, indextypename: IndexTypeName)(nnq: NearestNeighbourQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(implicit ac: AdamContext) {
    this(
      Entity.load(entityname).get.indexes
        .filter(_.isSuccess)
        .map(_.get)
        .filter(_.isQueryConform(nnq)) //choose only indexes that are conform to query
        .sortBy(index => -ScanWeightInspector(index))
        .head
    )(nnq, id)(filterExpr)
  }

  override protected def run(options: Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("performing index scan operation")

    ac.sc.setLocalProperty("spark.scheduler.pool", "index")
    ac.sc.setJobGroup(id.getOrElse(""), "index scan: " + index.indextypename.name, interruptOnCancel = true)

    if (!index.isQueryConform(nnq)) {
      throw QueryNotConformException()
    }

    //TODO: check if is query conform

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