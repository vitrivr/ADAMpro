package ch.unibas.dmi.dbis.adam.query.datastructures

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.config.FieldNames
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
    override protected def run(filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
      import SparkStartup.Implicits._
      val rdd = sc.emptyRDD[Row]
      sqlContext.createDataFrame(rdd, Result.resultSchema(""))
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
    override protected def run(filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
      val (results, pk) = parallelExec(filter)

      val rdd = ac.sc.parallelize(results.map(res => Row(res.distance, res.tid)))
      ac.sqlContext.createDataFrame(rdd, Result.resultSchema(pk))
    }

    /**
      *
      * @return
      */
    private def parallelExec(filter: Option[DataFrame]): (Seq[Result], String) = {
      val lfut = Future(l.evaluate(filter))
      val rfut = Future(r.evaluate(filter))

      val f = for (leftResult <- lfut; rightResult <- rfut) yield ({
        aggregate(leftResult, rightResult, leftResult.schema.fields.filterNot(_.name == FieldNames.distanceColumnName).head.name)
      })

      Await.result(f, Duration(100, TimeUnit.SECONDS))
    }

    /**
      *
      * @param leftResult
      * @param rightResult
      * @return
      */
    private def aggregate(leftResult: DataFrame, rightResult: DataFrame, pk : String): (Seq[Result], String) = {
      val left = leftResult.map(r => r.getAs[Long](pk)).collect()
      val right = rightResult.map(r => r.getAs[Long](pk)).collect()
      val result = left.union(right).map(id => new Result(0.toFloat, id))
      (result, pk)
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
    override protected def run(filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
      val (results, pk) = order match {
        case ExpressionEvaluationOrder.LeftFirst => leftFirstExec(filter)
        case ExpressionEvaluationOrder.RightFirst => rightFirstExec(filter)
        case ExpressionEvaluationOrder.Parallel => parallelExec(filter)
      }

      val rdd = ac.sc.parallelize(results.map(res => Row(res.distance, res.tid)))
      ac.sqlContext.createDataFrame(rdd, Result.resultSchema(pk))
    }

    /**
      *
      * @return
      */
    private def leftFirstExec(filter: Option[DataFrame]): (Seq[Result], String) = {
      val leftResult = l.evaluate(filter)
      val pk = leftResult.schema.fields.filterNot(_.name == FieldNames.distanceColumnName).head.name
      val rightResult = r.evaluate(Some(leftResult.select(pk)))

      aggregate(leftResult, rightResult, pk)
    }

    /**
      *
      * @return
      */
    private def rightFirstExec(filter: Option[DataFrame]): (Seq[Result], String) = {
      val rightResult = r.evaluate(filter)
      val pk = rightResult.schema.fields.filterNot(_.name == FieldNames.distanceColumnName).head.name
      val leftResult = l.evaluate(Some(rightResult.select(pk)))

      aggregate(leftResult, rightResult, pk)
    }

    /**
      *
      * @return
      */
    private def parallelExec(filter: Option[DataFrame]): (Seq[Result], String) = {
      val lfut = Future(l.evaluate(filter))
      val rfut = Future(r.evaluate(filter))

      val f = for (leftResult <- lfut; rightResult <- rfut) yield ({
        aggregate(leftResult, rightResult, leftResult.schema.fields.filterNot(_.name == FieldNames.distanceColumnName).head.name)
      })

      Await.result(f, Duration(100, TimeUnit.SECONDS))
    }

    /**
      *
      * @param leftResult
      * @param rightResult
      * @return
      */
    private def aggregate(leftResult: DataFrame, rightResult: DataFrame, pk : String): (Seq[Result], String) = {
      val left = leftResult.map(r => r.getAs[Long](pk)).collect()
      val right = rightResult.map(r => r.getAs[Long](pk)).collect()
      val result = left.intersect(right).map(id => new Result(0.toFloat, id))
      (result, pk)
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
    override protected def run(filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
      val (results, pk) = order match {
        case ExpressionEvaluationOrder.LeftFirst => leftFirstExec(filter)
        case ExpressionEvaluationOrder.RightFirst => rightFirstExec(filter)
        case ExpressionEvaluationOrder.Parallel => parallelExec(filter)
      }

      val rdd = ac.sc.parallelize(results.map(res => Row(res.distance, res.tid)))
      ac.sqlContext.createDataFrame(rdd, Result.resultSchema(pk))
    }

    /**
      *
      * @return
      */
    private def leftFirstExec(filter: Option[DataFrame]): (Seq[Result], String) = {
      val leftResult = l.evaluate(filter)
      val pk = leftResult.schema.fields.filterNot(_.name == FieldNames.distanceColumnName).head.name
      val rightResult = r.evaluate(Some(leftResult.select(pk)))

      aggregate(leftResult, rightResult, pk)
    }

    /**
      *
      * @return
      */
    private def rightFirstExec(filter: Option[DataFrame]):  (Seq[Result], String) = {
      val rightResult = r.evaluate(filter)
      val pk = rightResult.schema.fields.filterNot(_.name == FieldNames.distanceColumnName).head.name
      val leftResult = l.evaluate(Some(rightResult.select(pk)))

      aggregate(leftResult, rightResult, pk)
    }

    /**
      *
      * @return
      */
    private def parallelExec(filter: Option[DataFrame]):  (Seq[Result], String) = {
      val lfut = Future(l.evaluate(filter))
      val rfut = Future(r.evaluate(filter))

      val f = for (leftResult <- lfut; rightResult <- rfut) yield ({
        aggregate(leftResult, rightResult, leftResult.schema.fields.filterNot(_.name == FieldNames.distanceColumnName).head.name)
      })

      Await.result(f, Duration(100, TimeUnit.SECONDS))
    }

    /**
      *
      * @param leftResult
      * @param rightResult
      * @return
      */
    private def aggregate(leftResult: DataFrame, rightResult: DataFrame, pk : String):  (Seq[Result], String) = {
      val left = leftResult.map(r => r.getAs[Long](pk)).collect()
      val right = rightResult.map(r => r.getAs[Long](pk)).collect()
      val result = (left.toSet -- right.toSet).map(id => new Result(0.toFloat, id)).toSeq
      (result, pk)
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
