package ch.unibas.dmi.dbis.adam.rpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.api.QueryOp
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints
import ch.unibas.dmi.dbis.adam.query.progressive.ProgressiveQueryStatus
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
private[rpc] object SearchRPCMethods {
  /**
    *
    * @param request
    * @return
    */
  def runStandardQuery(request: SimpleQueryMessage): DataFrame = {
    val entity = request.entity
    val hint = QueryHints.withName(request.hint)
    val nnq = prepareNNQ(request.nnq)
    val bq = prepareBQ(request.bq)
    val meta = request.withMetadata

    QueryOp(entity, hint, nnq, bq, meta)
  }

  /**
    *
    * @param request
    * @return
    */
  def runSequentialQuery(request: SimpleSequentialQueryMessage): DataFrame = {
    val entity = request.entity

    val nnq = prepareNNQ(request.nnq)
    val bq = prepareBQ(request.bq)
    val meta = request.withMetadata

    QueryOp.sequential(entity, nnq, bq, meta)
  }

  /**
    *
    * @param request
    * @return
    */
  def runIndexQuery(request: SimpleIndexQueryMessage): DataFrame = {
    val entity = request.entity
    val indextype = IndexTypes.withIndextype(request.indextype)
    if (indextype.isEmpty) {
      throw new Exception("No existing index type specified.")
    }
    val nnq = prepareNNQ(request.nnq)
    val bq = prepareBQ(request.bq)
    val meta = request.withMetadata
    QueryOp.index(entity, indextype.get, nnq, bq, meta)
  }

  /**
    *
    * @param request
    * @return
    */
  def runSpecifiedIndexQuery(request: SimpleSpecifiedIndexQueryMessage): DataFrame = {
    val index = request.index
    val nnq = prepareNNQ(request.nnq)
    val bq = prepareBQ(request.bq)
    val meta = request.withMetadata

    QueryOp.index(index, nnq, bq, meta)
  }

  /**
    *
    * @param request
    * @return
    */
  def runProgressiveQuery(request: SimpleQueryMessage, onComplete : (ProgressiveQueryStatus.Value, DataFrame, VectorBase, String, Map[String, String]) => Unit): Unit = {
    val entity = request.entity
    val nnq = SearchRPCMethods.prepareNNQ(request.nnq)
    val bq = prepareBQ(request.bq)
    val meta = request.withMetadata

    QueryOp.progressive(entity, nnq, bq, onComplete, meta)
  }

  /**
    *
    * @param request
    */
  def runTimedProgressiveQuery(request: TimedQueryMessage) = {
    val entity = request.entity
    val time = request.time
    val nnq = prepareNNQ(request.nnq)
    val bq = prepareBQ(request.bq)
    val meta = request.withMetadata

    QueryOp.timedProgressive(entity, nnq, bq, Duration(time, TimeUnit.MILLISECONDS), meta)
  }

  /**
    *
    * @param option
    * @return
    */
  private def prepareNNQ(option: Option[NearestNeighbourQueryMessage]): NearestNeighbourQuery = {
    if (option.isEmpty) {
      throw new Exception("No kNN query specified.")
    }

    val nnq = option.get
    NearestNeighbourQuery(nnq.query, NormBasedDistanceFunction(nnq.norm), nnq.k, nnq.indexOnly, nnq.options)
  }

  /**
    *
    * @param option
    * @return
    */
  private def prepareBQ(option: Option[BooleanQueryMessage]): Option[BooleanQuery] = {
    if (option.isDefined) {
      val bq = option.get
      Option(BooleanQuery(bq.where.map(bqm => (bqm.field, bqm.value)), Option(bq.joins.map(x => (x.table, x.columns))), Option(bq.prefilter)))
    } else {
      None
    }
  }
}

private[rpc] object CompoundSearchRPCMethods {
  /**
    *
    * @param request
    */
  def runCompoundQuery(request: ExpressionQueryMessage): DataFrame = {
    eval(request)
  }

  /**
    *
    * @param request
    */
  def eval(request: ExpressionQueryMessage): DataFrame = {
    val f = op(request)(request.left, request.right)
    Await.result(f, Duration(100, TimeUnit.SECONDS))
  }

  /**
    *
    * @param expr
    * @return
    */
  def eval(expr: ExpressionQueryMessage.Left): DataFrame = expr match {
    case ExpressionQueryMessage.Left.Leqm(x) => eval(x)
    case ExpressionQueryMessage.Left.Lsiqm(x) => eval(x)
    case ExpressionQueryMessage.Left.Lssqm(x) => eval(x: SimpleSequentialQueryMessage)
    case ExpressionQueryMessage.Left.Empty => null
  }

  /**
    *
    * @param expr
    * @return
    */
  def eval(expr: ExpressionQueryMessage.Right): DataFrame = expr match {
    case ExpressionQueryMessage.Right.Reqm(x) => eval(x)
    case ExpressionQueryMessage.Right.Rsiqm(x) => eval(x)
    case ExpressionQueryMessage.Right.Rssqm(x) => eval(x: SimpleSequentialQueryMessage)
    case ExpressionQueryMessage.Right.Empty => null
  }


  /**
    *
    * @param request
    */
  def eval(request: SimpleIndexQueryMessage): DataFrame = {
    SearchRPCMethods.runIndexQuery(request)
  }

  /**
    *
    * @param request
    */
  def eval(request: SimpleSequentialQueryMessage): DataFrame = {
    SearchRPCMethods.runSequentialQuery(request)
  }

  /**
    *
    * @param request
    * @return
    */
  def op(request: ExpressionQueryMessage): (ExpressionQueryMessage.Left, ExpressionQueryMessage.Right) => Future[DataFrame] = request.operation match {
    case ExpressionQueryMessage.Operation.UNION => union
    case ExpressionQueryMessage.Operation.INTERSECT => intersect
    case ExpressionQueryMessage.Operation.EXCEPT => except
    case _ => null //TODO: do we need a pre-filter option?
  }

  /**
    *
    * @param left
    * @param right
    * @return
    */
  def union(left: ExpressionQueryMessage.Left, right: ExpressionQueryMessage.Right): Future[DataFrame] = {
    val lfut = Future {
      eval(left).select(FieldNames.idColumnName)
    }
    val rfut = Future {
      eval(right).select(FieldNames.idColumnName)
    }

    for {
      leftResult <- lfut
      rightResult <- rfut
    } yield ({
      leftResult.unionAll(rightResult).dropDuplicates(Seq(FieldNames.idColumnName))
    })
  }

  /**
    *
    * @param left
    * @param right
    * @return
    */
  def intersect(left: ExpressionQueryMessage.Left, right: ExpressionQueryMessage.Right): Future[DataFrame] = {
    val leftFuture = Future {
      eval(left).select(FieldNames.idColumnName)
    }
    val rightFuture = Future {
      eval(right).select(FieldNames.idColumnName)
    }

    for {
      leftResult <- leftFuture
      rightResult <- rightFuture
    } yield ({
      leftResult.intersect(rightResult)
    })
  }

  /**
    *
    * @param left
    * @param right
    * @return
    */
  def except(left: ExpressionQueryMessage.Left, right: ExpressionQueryMessage.Right): Future[DataFrame] = {
    val leftFuture = Future {
      eval(left).select(FieldNames.idColumnName)
    }
    val rightFuture = Future {
      eval(right).select(FieldNames.idColumnName)
    }

    for {
      leftResult <- leftFuture
      rightResult <- rightFuture
    } yield ({
      leftResult.except(rightResult)
    })
  }
}
