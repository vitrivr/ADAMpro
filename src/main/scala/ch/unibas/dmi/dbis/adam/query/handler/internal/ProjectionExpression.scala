package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.handler.internal.ProjectionExpression.ProjectionField
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class ProjectionExpression(private val projection: ProjectionField, private val expr: QueryExpression, id: Option[String] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Projection Expression"), id, None)
  _children ++= Seq(expr)

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("performing projection on data")
    expr.evaluate(options).map(projection.f)
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: ProjectionExpression => this.expr.equals(that.expr) && this.projection.equals(that.projection)
      case _ => this.expr.equals(that)
    }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + expr.hashCode
    result = prime * result + projection.hashCode
    result
  }
}

object ProjectionExpression extends Logging {

  abstract class ProjectionField {
    def f(df: DataFrame): DataFrame
  }

  case class FieldNameProjection(names: Seq[String])(implicit ac: AdamContext) extends ProjectionField {
    override def f(df: DataFrame): DataFrame = {
      if (names.nonEmpty && names.head != "*") {
        import org.apache.spark.sql.functions.col
        df.select(names.map(col): _*)
      } else {
        df
      }
    }

    override def equals(that: Any): Boolean =
      that match {
        case that: FieldNameProjection => this.names.sorted.equals(that.names.sorted)
        case _ => false
      }

    override def hashCode(): Int = {
      names.hashCode
    }
  }

  case class CountOperationProjection(implicit ac: AdamContext) extends ProjectionField {
    override def f(df: DataFrame): DataFrame = {
      ac.sqlContext.createDataFrame(ac.sc.makeRDD(Seq(Row(df.count()))), StructType(Seq(StructField("count", LongType))))
    }

    override def equals(that: Any): Boolean =
      that match {
        case that: CountOperationProjection => true
        case _ => false
      }

    override def hashCode(): Int = 0
  }

  case class ExistsOperationProjection(implicit ac: AdamContext) extends ProjectionField {
    override def f(df: DataFrame): DataFrame = {
      ac.sqlContext.createDataFrame(ac.sc.makeRDD(Seq(Row(df.count() > 1))), StructType(Seq(StructField("exists", BooleanType))))
    }

    override def equals(that: Any): Boolean =
      that match {
        case that: ExistsOperationProjection => true
        case _ => false
      }

    override def hashCode(): Int = 1
  }

  case class DistinctOperationProjection(implicit ac: AdamContext) extends ProjectionField {
    override def f(df: DataFrame): DataFrame = {
      df.distinct()
    }

    override def equals(that: Any): Boolean =
      that match {
        case that: DistinctOperationProjection => true
        case _ => false
      }

    override def hashCode(): Int = 0
  }

}