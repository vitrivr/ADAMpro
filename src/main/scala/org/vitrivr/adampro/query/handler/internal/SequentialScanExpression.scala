package org.vitrivr.adampro.query.handler.internal

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.vitrivr.adampro.config.FieldNames
import org.vitrivr.adampro.datatypes.feature.FeatureVectorWrapper
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.exception.QueryNotConformException
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.handler.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.query.{Predicate, NearestNeighbourQuery}
import org.vitrivr.adampro.utils.Logging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class SequentialScanExpression(private val entity: Entity)(private val nnq: NearestNeighbourQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(Some(entity.entityname), Some("Sequential Scan Expression"), id, None)
  val sourceDescription = {
    if (filterExpr.isDefined) {
      filterExpr.get.info.scantype.getOrElse("undefined") + "->" + info.scantype.getOrElse("undefined")
    } else {
      info.scantype.getOrElse("undefined")
    }
  }

  _children ++= filterExpr.map(Seq(_)).getOrElse(Seq())

  def this(entityname: EntityName)(nnq: NearestNeighbourQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(implicit ac: AdamContext) {
    this(Entity.load(entityname).get)(nnq, id)(filterExpr)
  }

  override protected def run(options: Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("perform sequential scan")

    ac.sc.setLocalProperty("spark.scheduler.pool", "sequential")
    ac.sc.setJobGroup(id.getOrElse(""), "sequential scan: " + entity.entityname.toString, interruptOnCancel = true)

    if (!nnq.isConform(entity)){
      throw QueryNotConformException("query is not conform to entity")
    }

    var ids = mutable.ListBuffer[Any]()

    log.trace(QUERY_MARKER, "preparing filtering ids")

    if (filter.isDefined) {
      ids ++= filter.get.select(entity.pk.name).collect().map(_.getAs[Any](entity.pk.name))
    }

    if (filterExpr.isDefined) {
      filterExpr.get.filter = filter
      ids ++= filterExpr.get.evaluate(options).get.select(entity.pk.name).collect().map(_.getAs[Any](entity.pk.name))
    }
    log.trace(QUERY_MARKER, "after preparing filtering ids")

    log.trace(QUERY_MARKER, "get data")

    var result = if (ids.nonEmpty) {
      entity.getData(predicates = Seq(new Predicate(entity.pk.name, None, ids.toSeq)))
    } else {
      entity.getData()
    }

    log.trace(QUERY_MARKER, "after get data")

    if (result.isDefined && options.isDefined && options.get.storeSourceProvenance) {
      result = Some(result.get.withColumn(FieldNames.sourceColumnName, lit(sourceDescription)))
    }

    result.map(SequentialScanExpression.scan(_, nnq))
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: SequentialScanExpression => this.entity.entityname.equals(that.entity.entityname) && this.nnq.equals(that.nnq)
      case _ => false
    }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + entity.hashCode
    result = prime * result + nnq.hashCode
    result
  }
}

object SequentialScanExpression extends Logging {

  /**
    * Scans the feature data based on a nearest neighbour query.
    *
    * @param df  data frame
    * @param nnq nearest neighbour query
    * @return
    */
  def scan(df: DataFrame, nnq: NearestNeighbourQuery)(implicit ac: AdamContext): DataFrame = {
    val q = ac.sc.broadcast(nnq.q)
    val w = ac.sc.broadcast(nnq.weights)

    import org.apache.spark.sql.functions.{col, udf}
    val distUDF = udf((c: FeatureVectorWrapper) => {
      try {
        if (c != null) {
          nnq.distance(q.value, c.vector, w.value).toFloat
        } else {
          Float.MaxValue
        }
      } catch {
        case e: Exception =>
          log.error("error when computing distance", e)
          Float.MaxValue
      }
    })

    log.trace(QUERY_MARKER, "executing distance computation")

    val res = df.withColumn(FieldNames.distanceColumnName, distUDF(df(nnq.attribute)))
      .orderBy(col(FieldNames.distanceColumnName))
      .limit(nnq.k)

    log.trace(QUERY_MARKER, "finished executing distance computation")

    res
  }
}



