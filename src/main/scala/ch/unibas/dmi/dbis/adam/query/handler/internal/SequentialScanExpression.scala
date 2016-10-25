package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.query.{Predicate, NearestNeighbourQuery}
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable

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

    var ids = mutable.HashSet[Any]()

    if (filter.isDefined) {
      ids ++= filter.get.select(entity.pk.name).collect().map(_.getAs[Any](entity.pk.name))
    }

    if (filterExpr.isDefined) {
      filterExpr.get.filter = filter
      ids ++= filterExpr.get.evaluate(options).get.select(entity.pk.name).collect().map(_.getAs[Any](entity.pk.name))
    }

    var result = if (ids.nonEmpty) {
      val data = entity.getData(predicates = Seq(new Predicate(entity.pk.name, None, ids.toSeq)))
      val idsbc = ac.sc.broadcast(ids)

      data.map(d => {
        val rdd = d.rdd.filter(x => idsbc.value.contains(x.getAs[Any](entity.pk.name)))
        ac.sqlContext.createDataFrame(rdd, d.schema)
      })
    } else {
      entity.getData()
    }

    //TODO: possibly join is faster? possibly the current implmentation is

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

    df.withColumn(FieldNames.distanceColumnName, distUDF(df(nnq.attribute)))
      .orderBy(col(FieldNames.distanceColumnName))
      .limit(nnq.k)
  }
}



