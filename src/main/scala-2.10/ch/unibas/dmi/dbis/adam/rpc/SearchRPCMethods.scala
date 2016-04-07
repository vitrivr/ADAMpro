package ch.unibas.dmi.dbis.adam.rpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.api.QueryOp._
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.query.handler.CompoundQueryHandler.{ExpressionEvaluationOrder, CompoundQueryHolder, Expression}
import ch.unibas.dmi.dbis.adam.query.handler.QueryHandler._
import ch.unibas.dmi.dbis.adam.query.handler.{CompoundQueryHandler, QueryHints}
import ch.unibas.dmi.dbis.adam.query.progressive.ProgressiveQueryStatus
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
private[rpc] object SearchRPCMethods {
  /* implicits */

  implicit def toQueryHolder(request: SimpleQueryMessage) = StandardQueryHolder(request.entity, QueryHints.withName(request.hint), prepareNNQ(request.nnq), prepareBQ(request.bq), request.withMetadata)

  implicit def toQueryHolder(request: SimpleSequentialQueryMessage) = new SequentialQueryHolder(request.entity, prepareNNQ(request.nnq), prepareBQ(request.bq), request.withMetadata)

  implicit def toQueryHolder(request: SimpleIndexQueryMessage) = new IndexQueryHolder(request.entity, IndexTypes.withIndextype(request.indextype).get, prepareNNQ(request.nnq), prepareBQ(request.bq), request.withMetadata)

  implicit def toQueryHolder(request: SimpleSpecifiedIndexQueryMessage) = new SpecifiedIndexQueryHolder(request.index, prepareNNQ(request.nnq), prepareBQ(request.bq), request.withMetadata)

  implicit def toQueryHolder(request: TimedQueryMessage) = new TimedProgressiveQueryHolder(request.entity, prepareNNQ(request.nnq), prepareBQ(request.bq), Duration(request.time, TimeUnit.MILLISECONDS), request.withMetadata)

  implicit def toQueryHolder(request: SimpleQueryMessage, onComplete: (ProgressiveQueryStatus.Value, DataFrame, VectorBase, String, Map[String, String]) => Unit) = new ProgressiveQueryHolder(request.entity, prepareNNQ(request.nnq), prepareBQ(request.bq), onComplete, request.withMetadata)

  implicit def toQueryHolder(request: CompoundQueryMessage) = {
    new CompoundQueryHolder(request.entity, prepareNNQ(request.nnq), toExpr(request.indexFilterExpression.get.submessage), request.withMetadata)
  }

  implicit def toExpr(request: ExpressionQueryMessage): Expression = {
    val order = request.order match {
      case ExpressionQueryMessage.OperationOrder.LEFTFIRST => ExpressionEvaluationOrder.LeftFirst
      case ExpressionQueryMessage.OperationOrder.RIGHTFIRST => ExpressionEvaluationOrder.RightFirst
      case ExpressionQueryMessage.OperationOrder.PARALLEL => ExpressionEvaluationOrder.Parallel
      case _ => null
    }

    val operation = request.operation match {
      case ExpressionQueryMessage.Operation.UNION => CompoundQueryHandler.UnionExpression(request.left.get.submessage, request.right.get.submessage)
      case ExpressionQueryMessage.Operation.INTERSECT => CompoundQueryHandler.IntersectExpression(request.left.get.submessage, request.right.get.submessage, order)
      case ExpressionQueryMessage.Operation.EXCEPT => CompoundQueryHandler.ExceptExpression(request.left.get.submessage, request.right.get.submessage, order)
      case _ => null //TODO: do we need a pre-filter option?
    }

    operation
  }

  implicit def toExpr(expr: SubExpressionQueryMessage.Submessage): Expression = expr match {
    case SubExpressionQueryMessage.Submessage.Eqm(x) => toExpr(x)
    case SubExpressionQueryMessage.Submessage.Ssiqm(x) => (x: SpecifiedIndexQueryHolder)
    case SubExpressionQueryMessage.Submessage.Siqm(x) => (x: IndexQueryHolder)
    case SubExpressionQueryMessage.Submessage.Ssqm(x) => (x: SequentialQueryHolder)
    case _ => null
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
      Option(BooleanQuery(Option(bq.where.map(bqm => (bqm.field, bqm.value))), Option(bq.joins.map(x => (x.table, x.columns))), Option(bq.prefilter.toSet)))
    } else {
      None
    }
  }
}

