package ch.unibas.dmi.dbis.adam.query.datastructures

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
import ch.unibas.dmi.dbis.adam.query.Result
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
object CompoundQueryExpressions {
  /**
    *
    */
  object ExpressionEvaluationOrder extends Enumeration {
    type Order = Value
    val LeftFirst, RightFirst, Parallel = Value
  }

  case class EmptyQueryExpression(id : Option[String] = None) extends QueryExpression(id){
    override protected def run(filter: Option[Set[TupleID]]): DataFrame = {
      import SparkStartup.Implicits._
      val rdd = sc.emptyRDD[Row]
      sqlContext.createDataFrame(rdd, Result.resultSchema)
    }
  }

  /**
    *
    * @param l
    * @param r
    */
  case class UnionExpression(l: QueryExpression, r: QueryExpression, id : Option[String] = None)(implicit ac : AdamContext) extends QueryExpression(id) {
    /**
      *
      * @param filter
      * @return
      */
    override protected def run(filter: Option[Set[TupleID]]): DataFrame = {
      val results = parallelExec(filter)

      val rdd = ac.sc.parallelize(results.map(res => Row(res.distance, res.tid)))
      ac.sqlContext.createDataFrame(rdd, Result.resultSchema)
    }

    /**
      *
      * @return
      */
    private def parallelExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val lfut = Future(l.evaluate(filter))
      val rfut = Future(r.evaluate(filter))

      val f = for (leftResult <- lfut; rightResult <- rfut) yield ({
        aggregate(leftResult, rightResult)
      })

      Await.result(f, Duration(100, TimeUnit.SECONDS))
    }

    /**
      *
      * @param leftResult
      * @param rightResult
      * @return
      */
    private def aggregate(leftResult: DataFrame, rightResult: DataFrame): Seq[Result] = {
      val left = leftResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect()
      val right = rightResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect()
      left.union(right).map(id => new Result(0.toFloat, id))
    }

    /**
      *
      * @param info
      * @return
      */
    override private[query] def getRunDetails(info : ListBuffer[RunDetails]) = {
      super.getRunDetails(info)
      l.getRunDetails(info)
      r.getRunDetails(info)
      info
    }
  }

  /**
    *
    * @param l
    * @param r
    */
  case class IntersectExpression(l: QueryExpression, r: QueryExpression, order: ExpressionEvaluationOrder.Order = ExpressionEvaluationOrder.Parallel, id : Option[String] = None)(implicit ac : AdamContext) extends QueryExpression(id) {
    /**
      *
      * @param filter
      * @return
      */
    override protected def run(filter: Option[Set[TupleID]]): DataFrame = {
      val results = order match {
        case ExpressionEvaluationOrder.LeftFirst => leftFirstExec(filter)
        case ExpressionEvaluationOrder.RightFirst => rightFirstExec(filter)
        case ExpressionEvaluationOrder.Parallel => parallelExec(filter)
      }

      val rdd = ac.sc.parallelize(results.map(res => Row(res.distance, res.tid)))
      ac.sqlContext.createDataFrame(rdd, Result.resultSchema)
    }

    /**
      *
      * @return
      */
    private def leftFirstExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val leftResult = l.evaluate(filter)
      val rightResult = r.evaluate(Option(leftResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect().toSet))

      aggregate(leftResult, rightResult)
    }

    /**
      *
      * @return
      */
    private def rightFirstExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val rightResult = r.evaluate(filter)
      val leftResult = l.evaluate(Option(rightResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect().toSet))

      aggregate(leftResult, rightResult)
    }

    /**
      *
      * @return
      */
    private def parallelExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val lfut = Future(l.evaluate(filter))
      val rfut = Future(r.evaluate(filter))

      val f = for (leftResult <- lfut; rightResult <- rfut) yield ({
        aggregate(leftResult, rightResult)
      })

      Await.result(f, Duration(100, TimeUnit.SECONDS))
    }

    /**
      *
      * @param leftResult
      * @param rightResult
      * @return
      */
    private def aggregate(leftResult: DataFrame, rightResult: DataFrame): Seq[Result] = {
      val left = leftResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect()
      val right = rightResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect()
      left.intersect(right).map(id => new Result(0.toFloat, id))
    }

    /**
      *
      * @param info
      * @return
      */
    override private[query] def getRunDetails(info : ListBuffer[RunDetails]) = {
      super.getRunDetails(info)
      l.getRunDetails(info)
      r.getRunDetails(info)
      info
    }
  }

  /**
    *
    * @param l
    * @param r
    */
  case class ExceptExpression(l: QueryExpression, r: QueryExpression, order: ExpressionEvaluationOrder.Order = ExpressionEvaluationOrder.Parallel, id : Option[String] = None)(implicit ac : AdamContext) extends QueryExpression(id) {
    /**
      *
      * @param filter
      * @return
      */
    override protected def run(filter: Option[Set[TupleID]]): DataFrame = {
      val results = order match {
        case ExpressionEvaluationOrder.LeftFirst => leftFirstExec(filter)
        case ExpressionEvaluationOrder.RightFirst => rightFirstExec(filter)
        case ExpressionEvaluationOrder.Parallel => parallelExec(filter)
      }

      val rdd = ac.sc.parallelize(results.map(res => Row(res.distance, res.tid)))
      ac.sqlContext.createDataFrame(rdd, Result.resultSchema)
    }

    /**
      *
      * @return
      */
    private def leftFirstExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val leftResult = l.evaluate(filter)
      val rightResult = r.evaluate(Option(leftResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect().toSet))

      aggregate(leftResult, rightResult)
    }

    /**
      *
      * @return
      */
    private def rightFirstExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val rightResult = r.evaluate(filter)
      val leftResult = l.evaluate(Option(rightResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect().toSet))

      aggregate(leftResult, rightResult)
    }

    /**
      *
      * @return
      */
    private def parallelExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val lfut = Future(l.evaluate(filter))
      val rfut = Future(r.evaluate(filter))

      val f = for (leftResult <- lfut; rightResult <- rfut) yield ({
        aggregate(leftResult, rightResult)
      })

      Await.result(f, Duration(100, TimeUnit.SECONDS))
    }

    /**
      *
      * @param leftResult
      * @param rightResult
      * @return
      */
    private def aggregate(leftResult: DataFrame, rightResult: DataFrame): Seq[Result] = {
      val left = leftResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect()
      val right = rightResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect()
      (left.toSet -- right.toSet).map(id => new Result(0.toFloat, id)).toSeq
    }

    /**
      *
      * @param info
      * @return
      */
    override private[query] def getRunDetails(info : ListBuffer[RunDetails]) = {
      super.getRunDetails(info)
      l.getRunDetails(info)
      r.getRunDetails(info)
      info
    }
  }
}
