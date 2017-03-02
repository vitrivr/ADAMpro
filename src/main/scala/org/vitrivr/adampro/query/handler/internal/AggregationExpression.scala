package org.vitrivr.adampro.query.handler.internal

import java.util.concurrent.TimeUnit

import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.AttributeTypes
import org.vitrivr.adampro.entity.AttributeDefinition
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.Result
import org.vitrivr.adampro.query.handler.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.handler.internal.AggregationExpression.ExpressionEvaluationOrder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.vitrivr.adampro.helpers.tracker.OperationTracker

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
abstract class AggregationExpression(private val left: QueryExpression, private val right: QueryExpression, order: ExpressionEvaluationOrder.Order = ExpressionEvaluationOrder.Parallel, options: Map[String, String] = Map(), id: Option[String] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  _children ++= Seq(left, right)

  override val info = ExpressionDetails(None, Some("Aggregation Expression (" + aggregationName + ", " + order.toString + ")"), id, None)

  def aggregationName: String

  override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : OperationTracker)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("run aggregation operation " + aggregationName + " between " +  left.getClass.getName + " and " + right.getClass.getName  + "  ordered " + order.toString)

    ac.sc.setJobGroup(id.getOrElse(""), "aggregation", interruptOnCancel = true)

    val result = order match {
      case ExpressionEvaluationOrder.LeftFirst => asymmetric(left, right, options, filter)(tracker)
      case ExpressionEvaluationOrder.RightFirst => asymmetric(right, left, options, filter)(tracker)
      case ExpressionEvaluationOrder.Parallel => symmetric(left, right, options, filter)(tracker)
    }

    Some(result)
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: AggregationExpression =>
        this.getClass().equals(that.getClass) && this.left.equals(that.left) && this.right.equals(that.right)
      case _ => false
    }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + this.getClass.hashCode
    result = prime * result + left.hashCode
    result = prime * result + right.hashCode
    result
  }

  private def asymmetric(first: QueryExpression, second: QueryExpression, options: Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : OperationTracker): DataFrame = {
    first.filter = filter
    var firstResult = first.evaluate(options)(tracker).get

    //TODO: possibly consider fuzzy sets rather than ignoring distance
    val pk = firstResult.schema.fields.filterNot(_.name == AttributeNames.distanceColumnName).head.name
    second.filter = Some(firstResult.select(pk))
    var secondResult = second.evaluate(options)(tracker).get

    var result = secondResult

    if (options.isDefined && options.get.storeSourceProvenance) {
      if(firstResult.columns.contains(AttributeNames.sourceColumnName)){
        firstResult = firstResult.withColumnRenamed(AttributeNames.sourceColumnName, AttributeNames.sourceColumnName + "-1")
      } else {
        firstResult = firstResult.withColumn(AttributeNames.sourceColumnName + "-1", lit(first.info.scantype.getOrElse("undefined")))
      }

      if(secondResult.columns.contains(AttributeNames.sourceColumnName)){
        secondResult = secondResult.withColumnRenamed(AttributeNames.sourceColumnName, AttributeNames.sourceColumnName + "-2")
      } else {
        secondResult = secondResult.withColumn(AttributeNames.sourceColumnName + "-2", lit(second.info.scantype.getOrElse("undefined")))
      }
      val sourceUDF = udf((s1: String, s2 : String) => s1 + "->" + s2)
      result = firstResult.select(pk, AttributeNames.sourceColumnName + "-1").join(secondResult, pk)
      result = result.withColumn(AttributeNames.sourceColumnName, sourceUDF(col(AttributeNames.sourceColumnName + "-1"), col(AttributeNames.sourceColumnName + "-2"))).drop(AttributeNames.sourceColumnName + "-1").drop(AttributeNames.sourceColumnName + "-2")
    }

    result
  }

  private def symmetric(first: QueryExpression, second: QueryExpression, options: Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : OperationTracker): DataFrame = {
    first.filter = filter
    val ffut = Future(first.evaluate(options)(tracker))
    second.filter = filter
    val sfut = Future(second.evaluate(options)(tracker))

    val f = for (firstResult <- ffut; secondResult <- sfut)
    //TODO: possilby consider fuzzy sets rather than ignoring distance
      yield aggregate(firstResult.get, secondResult.get, firstResult.get.schema.fields.filterNot(_.name == AttributeNames.distanceColumnName).head.name, options)

    var result = Await.result(f, Duration(100, TimeUnit.SECONDS))

    result
  }

  protected def aggregate(a: DataFrame, b: DataFrame, pk: String, options: Option[QueryEvaluationOptions]): DataFrame
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
  case class UnionExpression(l: QueryExpression, r: QueryExpression, options: Map[String, String] = Map(), id: Option[String] = None)(implicit ac: AdamContext) extends AggregationExpression(l, r, ExpressionEvaluationOrder.Parallel, options, id) {
    override def aggregationName = "UNION"

    override protected def aggregate(leftResult: DataFrame, rightResult: DataFrame, pk: String, options: Option[QueryEvaluationOptions]): DataFrame = {
      var left = leftResult
      var right = rightResult

      if (options.isDefined && options.get.storeSourceProvenance) {
        if(left.columns.contains(AttributeNames.sourceColumnName)){
          left = left.select(pk, AttributeNames.sourceColumnName)
        } else {
          left = left.select(pk).withColumn(AttributeNames.sourceColumnName, lit(l.info.scantype.getOrElse("undefined")))
        }

        if(right.columns.contains(AttributeNames.sourceColumnName)){
          right = right.select(pk, AttributeNames.sourceColumnName)
        } else {
          right = right.select(pk).withColumn(AttributeNames.sourceColumnName, lit(r.info.scantype.getOrElse("undefined")))
        }
      } else {
        left = left.select(pk)
        right = right.select(pk)
      }

      import org.apache.spark.sql.functions.lit
      left.unionAll(right).withColumn(AttributeNames.distanceColumnName, lit(0.toFloat))
    }
  }

  /**
    *
    * @param l     left expression
    * @param r     right expression
    * @param order execution order
    */
  case class IntersectExpression(l: QueryExpression, r: QueryExpression, order: ExpressionEvaluationOrder.Order = ExpressionEvaluationOrder.Parallel, options: Map[String, String] = Map(), id: Option[String] = None)(implicit ac: AdamContext) extends AggregationExpression(l, r, order, options, id) {
    override def aggregationName = "INTERSECT"

    override protected def aggregate(leftResult: DataFrame, rightResult: DataFrame, pk: String, options: Option[QueryEvaluationOptions]): DataFrame = {
      var left = leftResult
      var right = rightResult


      if (options.isDefined && options.get.storeSourceProvenance) {
        if(left.columns.contains(AttributeNames.sourceColumnName)){
          left = left.select(pk, AttributeNames.sourceColumnName)
        } else {
          left = left.select(pk).withColumn(AttributeNames.sourceColumnName, lit(l.info.scantype.getOrElse("undefined") + " " + aggregationName + " " + r.info.scantype.getOrElse("undefined")))
        }

        if(right.columns.contains(AttributeNames.sourceColumnName)){
          right = right.select(pk, AttributeNames.sourceColumnName)
        } else {
          right = right.select(pk).withColumn(AttributeNames.sourceColumnName, lit(l.info.scantype.getOrElse("undefined") + " " + aggregationName + " " + r.info.scantype.getOrElse("undefined")))
        }
      } else {
        left = left.select(pk)
        right = right.select(pk)
      }

      import org.apache.spark.sql.functions.lit
      left.intersect(right).withColumn(AttributeNames.distanceColumnName, lit(0.toFloat))
    }
  }

  /**
    *
    * @param l     left expression
    * @param r     right expression
    * @param order execution order
    */
  case class ExceptExpression(l: QueryExpression, r: QueryExpression, order: ExpressionEvaluationOrder.Order = ExpressionEvaluationOrder.Parallel, options: Map[String, String] = Map(), id: Option[String] = None)(implicit ac: AdamContext) extends AggregationExpression(l, r, order, options, id) {
    override def aggregationName = "EXCEPT"

    override protected def aggregate(leftResult: DataFrame, rightResult: DataFrame, pk: String, options: Option[QueryEvaluationOptions]): DataFrame = {
      var left = leftResult
      var right = rightResult

      if (options.isDefined && options.get.storeSourceProvenance) {
        if(left.columns.contains(AttributeNames.sourceColumnName)){
          left = left.select(pk, AttributeNames.sourceColumnName)
        } else {
          left = left.select(pk).withColumn(AttributeNames.sourceColumnName, lit(l.info.scantype.getOrElse("undefined") + " " + aggregationName + " " + r.info.scantype.getOrElse("undefined")))
        }

        if(right.columns.contains(AttributeNames.sourceColumnName)){
          right = right.select(pk, AttributeNames.sourceColumnName)
        } else {
          right = right.select(pk).withColumn(AttributeNames.sourceColumnName, lit(l.info.scantype.getOrElse("undefined") + " " + aggregationName + " " + r.info.scantype.getOrElse("undefined")))
        }
      } else {
        left = left.select(pk)
        right = right.select(pk)
      }


      import org.apache.spark.sql.functions.lit
      left.except(right).withColumn(AttributeNames.distanceColumnName, lit(0.toFloat))
    }
  }


  /**
    *
    */
  case class EmptyExpression(id: Option[String] = None) extends QueryExpression(id) {
    override val info = ExpressionDetails(None, Some("Empty Expression"), id, None)

    override protected def run(options : Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker : OperationTracker)(implicit ac: AdamContext): Option[DataFrame] = {
      val rdd = ac.sc.emptyRDD[Row]
      Some(ac.sqlContext.createDataFrame(rdd, Result.resultSchema))
    }

    override def equals(that: Any): Boolean =
      that match {
        case that: EmptyExpression => true
        case _ => false
      }

    override def hashCode: Int = 0
  }

}
