package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.handler.internal.ProjectionExpression.ProjectionField
import org.apache.spark.Logging
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class ProjectionExpression(projection: ProjectionField, expr : QueryExpression, id: Option[String] = None)(implicit ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Projection Expression"), id, None)
  children ++= Seq(expr)

  override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("performing projection on data")
    expr.evaluate().map(projection.f)
  }
}

object ProjectionExpression extends Logging {
  abstract class ProjectionField {
    def f(df: DataFrame): DataFrame
  }

  case class FieldNameProjection(names: Seq[String])(implicit ac: AdamContext) extends ProjectionField {
    override def f(df: DataFrame): DataFrame = {
      if (names.nonEmpty) {
        import org.apache.spark.sql.functions.col
        df.select(names.map(col): _*)
      } else {
        df
      }
    }
  }

  case class CountOperationProjection(implicit ac: AdamContext) extends ProjectionField {
    override def f(df: DataFrame): DataFrame = {
      ac.sqlContext.createDataFrame(ac.sc.makeRDD(Seq(Row(df.count()))), StructType(Seq(StructField("count", LongType))))
    }
  }

  case class ExistsOperationProjection(implicit ac: AdamContext) extends ProjectionField {
    override def f(df: DataFrame): DataFrame = {
      ac.sqlContext.createDataFrame(ac.sc.makeRDD(Seq(Row(df.count() > 1))), StructType(Seq(StructField("exists", BooleanType))))
    }
  }
}