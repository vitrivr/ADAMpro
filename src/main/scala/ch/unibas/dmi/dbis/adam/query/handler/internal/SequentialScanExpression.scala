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

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class SequentialScanExpression(entityname: EntityName)(nnq: NearestNeighbourQuery, id: Option[String] = None)(filterExpr : Option[QueryExpression] = None)(implicit ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(Some(entityname), Some("Sequential Scan Expression"), id, None)
  children ++= filterExpr.map(Seq(_)).getOrElse(Seq())

  override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    val entity = Entity.load(entityname).get
    if (!entity.isQueryConform(nnq)) {
      throw QueryNotConformException()
    }

    var df = entity.data

    if (filter.isDefined) {
      df = df.map(_.join(filter.get, entity.pk.name).select(entity.pk.name).join(entity.data.get, entity.pk.name))
    }

    if(filterExpr.isDefined){
      filterExpr.get.filter = filter
      df = df.map(_.join(filterExpr.get.evaluate().get, entity.pk.name).select(entity.pk.name).join(entity.data.get, entity.pk.name))
    }

    df.map(SequentialScanExpression.scan(_, nnq))
  }
}

object SequentialScanExpression extends Logging {

  /**
    * Scans the feature data based on a nearest neighbour query.
    *
    * @param df data frame
    * @param nnq nearest neighbour query
    * @return
    */
  def scan(df : DataFrame, nnq: NearestNeighbourQuery)(implicit ac: AdamContext): DataFrame = {
    val q = ac.sc.broadcast(nnq.q)

    import org.apache.spark.sql.functions.{col, udf}
    val distUDF = udf((c: FeatureVectorWrapper) => {
      try {
        if (c != null) {
          nnq.distance(q.value, c.vector)
        } else {
          Float.MaxValue
        }
      } catch {
        case e: Exception => {
          log.error("error when computing distance", e)
          Float.MaxValue
        }
      }
    })

    df.withColumn(FieldNames.distanceColumnName, distUDF(df(nnq.column)))
      .orderBy(col(FieldNames.distanceColumnName))
      .limit(nnq.k)
  }
}



