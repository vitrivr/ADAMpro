package ch.unibas.dmi.dbis.adam.query.handler

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.query.scanner.FeatureScanner
import org.apache.spark.sql.{DataFrame, Row}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
object CompoundQueryHandler {

  case class CompoundQueryHolder(entityname: EntityName, nnq: NearestNeighbourQuery, expr: Expression, withMetadata: Boolean) extends Expression {
    override def eval(): DataFrame = indexQueryWithResults(entityname)(nnq, expr, withMetadata)
  }

  /**
    *
    * @param entityname
    * @param nnq
    * @param expr
    * @param withMetadata
    * @return
    */
  def indexQueryWithResults(entityname: EntityName)(nnq: NearestNeighbourQuery, expr: Expression, withMetadata: Boolean): DataFrame = {
    val tidFilter = expr.eval().map(x => Result(0.toFloat, x.getAs[Long](FieldNames.idColumnName))).map(_.tid).collect().toSet
    var res = FeatureScanner(Entity.load(entityname).get, nnq, Some(tidFilter))

    if (withMetadata) {
      log.debug("join metadata to results of compound query")
      res = QueryHandler.joinWithMetadata(entityname, res)
    }

    res
  }

  /**
    *
    * @param expr
    * @return
    */
  def indexOnlyQuery(expr: Expression): DataFrame = expr.eval()


  /**
    *
    */
  abstract class Expression {
    /**
      *
      * @return
      */
    def eval(): DataFrame

    /**
      *
      * @param filter
      * @return
      */
    def eval(filter: Option[Set[TupleID]]): DataFrame = {
      val df = eval()
      if(filter.isDefined){
        df.filter(df(FieldNames.idColumnName) isin (filter.get.toSeq: _*))
      } else {
        df
      }
    }
  }

  /**
    *
    */
  object ExpressionEvaluationOrder extends Enumeration {
    type Order = Value
    val LeftFirst, RightFirst, Parallel = Value
  }

  /**
    *
    * @param l
    * @param r
    */
  case class UnionExpression(l: Expression, r: Expression) extends Expression {
    override def eval(): DataFrame = {
      eval(None)
    }

    override def eval(filter: Option[Set[TupleID]]): DataFrame = {
      val results = parallelExec(filter)

      val rdd = SparkStartup.sc.parallelize(results.map(res => Row(res.distance, res.tid)))
      SparkStartup.sqlContext.createDataFrame(rdd, Result.resultSchema)
    }

    /**
      *
      * @return
      */
    private def parallelExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val lfut = Future(l.eval(filter))
      val rfut = Future(r.eval(filter))

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
  }

  /**
    *
    * @param l
    * @param r
    */
  case class IntersectExpression(l: Expression, r: Expression, order: ExpressionEvaluationOrder.Order = ExpressionEvaluationOrder.Parallel) extends Expression {
    override def eval(): DataFrame = {
      eval(None)
    }

    override def eval(filter: Option[Set[TupleID]]): DataFrame = {
      val results = order match {
        case ExpressionEvaluationOrder.LeftFirst => leftFirstExec(filter)
        case ExpressionEvaluationOrder.RightFirst => rightFirstExec(filter)
        case ExpressionEvaluationOrder.Parallel => parallelExec(filter)
      }

      val rdd = SparkStartup.sc.parallelize(results.map(res => Row(res.distance, res.tid)))
      SparkStartup.sqlContext.createDataFrame(rdd, Result.resultSchema)
    }

    /**
      *
      * @return
      */
    private def leftFirstExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val leftResult = l.eval(filter)
      val rightResult = r.eval(Option(leftResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect().toSet))

      aggregate(leftResult, rightResult)
    }

    /**
      *
      * @return
      */
    private def rightFirstExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val rightResult = r.eval(filter)
      val leftResult = l.eval(Option(rightResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect().toSet))

      aggregate(leftResult, rightResult)
    }

    /**
      *
      * @return
      */
    private def parallelExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val lfut = Future(l.eval(filter))
      val rfut = Future(r.eval(filter))

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
  }

  /**
    *
    * @param l
    * @param r
    */
  case class ExceptExpression(l: Expression, r: Expression, order: ExpressionEvaluationOrder.Order = ExpressionEvaluationOrder.Parallel) extends Expression {
    override def eval(): DataFrame = {
      eval(None)
    }

    override def eval(filter: Option[Set[TupleID]]): DataFrame = {
      val results = order match {
        case ExpressionEvaluationOrder.LeftFirst => leftFirstExec(filter)
        case ExpressionEvaluationOrder.RightFirst => rightFirstExec(filter)
        case ExpressionEvaluationOrder.Parallel => parallelExec(filter)
      }

      val rdd = SparkStartup.sc.parallelize(results.map(res => Row(res.distance, res.tid)))
      SparkStartup.sqlContext.createDataFrame(rdd, Result.resultSchema)
    }

    /**
      *
      * @return
      */
    private def leftFirstExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val leftResult = l.eval(filter)
      val rightResult = r.eval(Option(leftResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect().toSet))

      aggregate(leftResult, rightResult)
    }

    /**
      *
      * @return
      */
    private def rightFirstExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val rightResult = r.eval(filter)
      val leftResult = l.eval(Option(rightResult.map(r => r.getAs[Long](FieldNames.idColumnName)).collect().toSet))

      aggregate(leftResult, rightResult)
    }

    /**
      *
      * @return
      */
    private def parallelExec(filter: Option[Set[TupleID]]): Seq[Result] = {
      val lfut = Future(l.eval(filter))
      val rfut = Future(r.eval(filter))

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
  }

}
