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
import org.scalatest
import org.scalatest.run

import scala.collection.mutable.ListBuffer
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

  case class CompoundQueryHolder(entityname: EntityName, nnq: NearestNeighbourQuery, expr: Expression, indexOnly : Boolean = false, withMetadata: Boolean, id: String = "") extends Expression(id) {
    var run = false

    override protected def run(filter: Option[Set[TupleID]]): DataFrame = {
      //TODO: catch here expr == null?
      
      val results = if(indexOnly){
        indexOnlyQuery(entityname)(expr, withMetadata)
      } else {
        indexQueryWithResults(entityname)(nnq, expr, withMetadata)
      }
      run = true
      results
    }

    /**
      *
      * @return
      */
    def provideRunInfo() : Seq[RunInfo] = {
      if(!run){
        log.warn("please run compound query before collecting run information")
        return Seq()
      } else {
        val start = collectRunInfo(new ListBuffer())
        expr.collectRunInfo(start).toSeq
      }
    }
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
    val tidFilter = expr.evaluate().map(x => Result(0.toFloat, x.getAs[Long](FieldNames.idColumnName))).map(_.tid).collect().toSet
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
  def indexOnlyQuery(entityname: EntityName = "")(expr: Expression, withMetadata: Boolean = false): DataFrame = {
    val tidFilter = expr.evaluate().map(x => Result(0.toFloat, x.getAs[Long](FieldNames.idColumnName))).map(_.tid).collect().toSet

    val rdd = SparkStartup.sc.parallelize(tidFilter.toSeq).map(res => Row(0.toFloat, res))
    var res = SparkStartup.sqlContext.createDataFrame(rdd, Result.resultSchema)

    if (withMetadata) {
      log.debug("join metadata to results of compound query")
      res = QueryHandler.joinWithMetadata(entityname, res)
    }

   res
  }

  case class RunInfo(id : String, time : Duration, results : DataFrame)

  /**
    *
    */
  abstract class Expression(id: String) {
    private var time: Duration = null
    private var results : DataFrame = null

    /**
      *
      * @param filter
      * @return
      */
    def evaluate(filter: Option[Set[TupleID]] = None): DataFrame = {
      val t1 = System.currentTimeMillis
      results = run(filter)
      val t2 = System.currentTimeMillis

      time = Duration(t2 - t1, TimeUnit.MILLISECONDS)

      results
    }

    /**
      *
      * @param filter
      * @return
      */
    protected def run(filter: Option[Set[TupleID]]): DataFrame


    /**
      *
      * @param info
      * @return
      */
    private[handler] def collectRunInfo(info : ListBuffer[RunInfo]) = {
      info += RunInfo(id, time, results)
    }
  }

  /**
    *
    */
  object ExpressionEvaluationOrder extends Enumeration {
    type Order = Value
    val LeftFirst, RightFirst, Parallel = Value
  }

  case class EmptyExpression(id : String = "") extends Expression(id){
    override protected def run(filter: Option[Set[TupleID]]): DataFrame = {
      val rdd = SparkStartup.sc.emptyRDD[Row]
      SparkStartup.sqlContext.createDataFrame(rdd, Result.resultSchema)
    }
  }

  /**
    *
    * @param l
    * @param r
    */
  case class UnionExpression(l: Expression, r: Expression, id: String = "") extends Expression(id) {
    /**
      *
      * @param filter
      * @return
      */
    override protected def run(filter: Option[Set[TupleID]]): DataFrame = {
      val results = parallelExec(filter)

      val rdd = SparkStartup.sc.parallelize(results.map(res => Row(res.distance, res.tid)))
      SparkStartup.sqlContext.createDataFrame(rdd, Result.resultSchema)
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
    override private[handler] def collectRunInfo(info : ListBuffer[RunInfo]) = {
      super.collectRunInfo(info)
      l.collectRunInfo(info)
      r.collectRunInfo(info)
      info
    }
  }

  /**
    *
    * @param l
    * @param r
    */
  case class IntersectExpression(l: Expression, r: Expression, order: ExpressionEvaluationOrder.Order = ExpressionEvaluationOrder.Parallel, id: String = "") extends Expression(id) {
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

      val rdd = SparkStartup.sc.parallelize(results.map(res => Row(res.distance, res.tid)))
      SparkStartup.sqlContext.createDataFrame(rdd, Result.resultSchema)
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
    override private[handler] def collectRunInfo(info : ListBuffer[RunInfo]) = {
      super.collectRunInfo(info)
      l.collectRunInfo(info)
      r.collectRunInfo(info)
      info
    }
  }

  /**
    *
    * @param l
    * @param r
    */
  case class ExceptExpression(l: Expression, r: Expression, order: ExpressionEvaluationOrder.Order = ExpressionEvaluationOrder.Parallel, id: String = "") extends Expression(id) {
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

      val rdd = SparkStartup.sc.parallelize(results.map(res => Row(res.distance, res.tid)))
      SparkStartup.sqlContext.createDataFrame(rdd, Result.resultSchema)
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
    override private[handler] def collectRunInfo(info : ListBuffer[RunInfo]) = {
      super.collectRunInfo(info)
      l.collectRunInfo(info)
      r.collectRunInfo(info)
      info
    }
  }

}
