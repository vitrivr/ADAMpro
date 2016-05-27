package ch.unibas.dmi.dbis.adam.query.handler.internal

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.handler.internal.AggregationExpression.ExpressionEvaluationOrder
import org.apache.spark.sql.{DataFrame, Row}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
abstract class AggregationExpression(left: QueryExpression, right: QueryExpression, order: ExpressionEvaluationOrder.Order = ExpressionEvaluationOrder.Parallel, id: Option[String] = None)(implicit ac: AdamContext) extends QueryExpression(id) {
  children ++= Seq(left, right)

  override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    val result = order match {
      case ExpressionEvaluationOrder.LeftFirst => asymmetric(left, right, filter)
      case ExpressionEvaluationOrder.RightFirst => asymmetric(right, left, filter)
      case ExpressionEvaluationOrder.Parallel => symmetric(left, right, filter)
    }

    Some(result)
  }

  private def asymmetric(first: QueryExpression, second: QueryExpression, filter: Option[DataFrame] = None): DataFrame = {
    first.filter = filter
    val firstResult = first.evaluate()

    val pk = firstResult.get.schema.fields.filterNot(_.name == FieldNames.distanceColumnName).head.name
    second.filter = firstResult.map(_.select(pk))
    val secondResult = second.evaluate()

    aggregate(firstResult.get, secondResult.get, pk)
  }

  private def symmetric(first: QueryExpression, second: QueryExpression, filter: Option[DataFrame] = None): DataFrame = {
    first.filter = filter
    val ffut = Future(first.evaluate())
    second.filter = filter
    val sfut = Future(second.evaluate())

    val f = for (firstResult <- ffut; secondResult <- sfut)
      yield aggregate(firstResult.get, secondResult.get, firstResult.get.schema.fields.filterNot(_.name == FieldNames.distanceColumnName).head.name)

    Await.result(f, Duration(100, TimeUnit.SECONDS))
  }

  protected def aggregate(a: DataFrame, b: DataFrame, pk: String): DataFrame
}


object AggregationExpression {

  /**
    *
    */
  object ExpressionEvaluationOrder extends Enumeration {
    type Order = Value
    val LeftFirst, RightFirst, Parallel = Value
  }


  /**
    *
    * @param l left expression
    * @param r right expression
    */
  case class UnionExpression(l: QueryExpression, r: QueryExpression, id: Option[String] = None)(implicit ac: AdamContext) extends AggregationExpression(l, r, ExpressionEvaluationOrder.Parallel, id) {
    override val info = ExpressionDetails(None, Some("Aggregation Expression (" + "UNION" + ")"), id, None)

    override protected def aggregate(leftResult: DataFrame, rightResult: DataFrame, pk: String): DataFrame = {
      val left = leftResult.map(r => r.getAs[Long](pk)).collect()
      val right = rightResult.map(r => r.getAs[Long](pk)).collect()
      val result = left.union(right).map(id => new Result(0.toFloat, id))

      val rdd = ac.sc.parallelize(result.map(res => Row(res.distance, res.tid)))
      ac.sqlContext.createDataFrame(rdd, Result.resultSchema(pk))
    }
  }

  /**
    *
    * @param l     left expression
    * @param r     right expression
    * @param order execution order
    */
  case class IntersectExpression(l: QueryExpression, r: QueryExpression, order: ExpressionEvaluationOrder.Order = ExpressionEvaluationOrder.Parallel, id: Option[String] = None)(implicit ac: AdamContext) extends AggregationExpression(l, r, order, id) {
    override val info = ExpressionDetails(None, Some("Aggregation Expression (" + "INTERSECT" + ")"), id, None)

    override protected def aggregate(leftResult: DataFrame, rightResult: DataFrame, pk: String): DataFrame = {
      val left = leftResult.map(r => r.getAs[Long](pk)).collect()
      val right = rightResult.map(r => r.getAs[Long](pk)).collect()
      val result = left.intersect(right).map(id => new Result(0.toFloat, id))

      val rdd = ac.sc.parallelize(result.map(res => Row(res.distance, res.tid)))
      ac.sqlContext.createDataFrame(rdd, Result.resultSchema(pk))
    }
  }

  /**
    *
    * @param l     left expression
    * @param r     right expression
    * @param order execution order
    */
  case class ExceptExpression(l: QueryExpression, r: QueryExpression, order: ExpressionEvaluationOrder.Order = ExpressionEvaluationOrder.Parallel, id: Option[String] = None)(implicit ac: AdamContext) extends AggregationExpression(l, r, order, id) {
    override val info = ExpressionDetails(None, Some("Aggregation Expression (" + "EXCEPT" + ")"), id, None)

    override protected def aggregate(leftResult: DataFrame, rightResult: DataFrame, pk: String): DataFrame = {
      val left = leftResult.map(r => r.getAs[Long](pk)).collect()
      val right = rightResult.map(r => r.getAs[Long](pk)).collect()
      val result = (left.toSet -- right.toSet).map(id => new Result(0.toFloat, id)).toSeq

      val rdd = ac.sc.parallelize(result.map(res => Row(res.distance, res.tid)))
      ac.sqlContext.createDataFrame(rdd, Result.resultSchema(pk))
    }
  }


  /**
    *
    */
  case class EmptyExpression(id: Option[String] = None) extends QueryExpression(id) {
    override val info = ExpressionDetails(None, Some("Empty Expression"), id, None)

    override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
      import SparkStartup.Implicits._
      val rdd = sc.emptyRDD[Row]
      Some(sqlContext.createDataFrame(rdd, Result.resultSchema("")))
    }
  }

}
