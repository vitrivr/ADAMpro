package org.vitrivr.adampro.query.ast.internal

import org.vitrivr.adampro.query.ast.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.ast.internal.ProjectionExpression.ProjectionField
import org.vitrivr.adampro.utils.Logging
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.tracker.QueryTracker

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class ProjectionExpression(private val projection: ProjectionField, private val qExpr: QueryExpression, id: Option[String] = None)(@transient implicit val ac: SharedComponentContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Projection Expression"), id, None)
  var expr = qExpr
    _children ++= Seq(expr)

  override def rewrite(silent : Boolean = false): QueryExpression = {
    super.rewrite(silent)
    expr = expr.rewrite(silent = true) //expr needs to be replaced
    this
  }

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : QueryTracker)(implicit ac: SharedComponentContext): Option[DataFrame] = {
    log.trace("performing projection on data")
    expr.execute(options)(tracker).map(projection.f)
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

  case class FieldNameProjection(names: Seq[String])(implicit ac: SharedComponentContext) extends ProjectionField {
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

  case class CountOperationProjection(implicit ac: SharedComponentContext) extends ProjectionField {
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

  case class ExistsOperationProjection(implicit ac: SharedComponentContext) extends ProjectionField {
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

  case class DistinctOperationProjection(implicit ac: SharedComponentContext) extends ProjectionField {
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