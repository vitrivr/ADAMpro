package ch.unibas.dmi.dbis.adam.rpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.api.QueryOp._
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.query.handler.CompoundQueryHandler.{CompoundQueryHolder, Expression}
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

  implicit def toQueryHolder(request: CompoundQueryMessage) = new CompoundQueryHolder(request.entity, prepareNNQ(request.nnq), toExpr(request.indexFilterExpression.get), request.withMetadata)

  implicit def toExpr(request: ExpressionQueryMessage) : Expression = request.operation match {
    case ExpressionQueryMessage.Operation.UNION => CompoundQueryHandler.UnionExpression(request.left, request.right)
    case ExpressionQueryMessage.Operation.INTERSECT => CompoundQueryHandler.IntersectExpression(request.left, request.right)
    case ExpressionQueryMessage.Operation.EXCEPT =>  CompoundQueryHandler.ExceptExpression(request.left, request.right)
    case _ => null //TODO: do we need a pre-filter option?
  }

  implicit def toExpr(expr: ExpressionQueryMessage.Left) : Expression = expr match {
    case ExpressionQueryMessage.Left.Leqm(x) => toExpr(x)
    case ExpressionQueryMessage.Left.Lsiqm(x) => (x: IndexQueryHolder)
    case ExpressionQueryMessage.Left.Lssqm(x) => (x: SequentialQueryHolder)
    case _ => null
  }


  implicit def toExpr(expr: ExpressionQueryMessage.Right) : Expression = expr match {
    case ExpressionQueryMessage.Right.Reqm(x) => toExpr(x)
    case ExpressionQueryMessage.Right.Rsiqm(x) => (x: IndexQueryHolder)
    case ExpressionQueryMessage.Right.Rssqm(x) => (x: SequentialQueryHolder)
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
      Option(BooleanQuery(bq.where.map(bqm => (bqm.field, bqm.value)), Option(bq.joins.map(x => (x.table, x.columns))), Option(bq.prefilter)))
    } else {
      None
    }
  }
}

