package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.QueryNotConformException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class SequentialScanExpression(entityname: EntityName)(nnq: NearestNeighbourQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(implicit ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(Some(entityname), Some("Sequential Scan Expression"), id, None)
  children ++= filterExpr.map(Seq(_)).getOrElse(Seq())

  override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("perform sequential scan")

    ac.sc.setLocalProperty("spark.scheduler.pool", "sequential")
    ac.sc.setJobGroup(id.getOrElse(""), "sequential scan: " + entityname.toString, interruptOnCancel = true)

    val entity = Entity.load(entityname).get
    if (!entity.isQueryConform(nnq)) {
      throw QueryNotConformException()
    }

    var df = entity.data
    var ids = mutable.Set[Any]()

    if (filter.isDefined) {
      ids ++= filter.get.select(entity.pk.name).collect().map(_.getAs[Any](entity.pk.name))
    }

    if (filterExpr.isDefined) {
      filterExpr.get.filter = filter
      ids ++= filterExpr.get.evaluate().get.select(entity.pk.name).collect().map(_.getAs[Any](entity.pk.name))
    }

    if (ids.nonEmpty) {
      val idsbc = ac.sc.broadcast(ids)
      df = df.map(d => {
        val rdd = d.rdd.filter(x => idsbc.value.contains(x.getAs[Any](entity.pk.name)))
        ac.sqlContext.createDataFrame(rdd, d.schema)
      })
    }

    df.map(SequentialScanExpression.scan(_, nnq))
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
    val weights = ac.sc.broadcast(nnq.weights)

    import org.apache.spark.sql.functions.{col, udf}
    val distUDF = udf((c: FeatureVectorWrapper) => {
      try {
        if (c != null) {
          nnq.distance(q.value, c.vector, weights.value)
        } else {
          Float.MaxValue
        }
      } catch {
        case e: Exception =>
          log.error("error when computing distance", e)
          Float.MaxValue
      }
    })

    df.withColumn(FieldNames.distanceColumnName, distUDF(df(nnq.column)))
      .orderBy(col(FieldNames.distanceColumnName))
      .limit(nnq.k)
  }
}



