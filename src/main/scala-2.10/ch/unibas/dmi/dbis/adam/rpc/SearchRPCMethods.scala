package ch.unibas.dmi.dbis.adam.rpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.http.grpc.DistanceMessage.DistanceType
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.query.datastructures.CompoundQueryExpressions._
import ch.unibas.dmi.dbis.adam.query.datastructures.{QueryCacheOptions, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, NormBasedDistanceFunction}
import ch.unibas.dmi.dbis.adam.query.handler.QueryHandler._
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints
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

  implicit def toQueryHolder(request: SimpleQueryMessage) = {

    StandardQueryHolder(request.entity, QueryHints.withName(request.hint), Option(prepareNNQ(request.nnq)), prepareBQ(request.bq), request.withMetadata, prepareQI(request.queryid), prepareCO(request.readFromCache, request.putInCache))
  }

  implicit def toQueryHolder(request: SimpleSequentialQueryMessage) = {
    new SequentialQueryHolder(request.entity, prepareNNQ(request.nnq), prepareBQ(request.bq), request.withMetadata, prepareQI(request.queryid), prepareCO(request.readFromCache, request.putInCache))
  }

  implicit def toQueryHolder(request: SimpleIndexQueryMessage) = {
    new IndexQueryHolder(request.entity, IndexTypes.withIndextype(request.indextype).get, prepareNNQ(request.nnq), prepareBQ(request.bq), request.withMetadata, prepareQI(request.queryid), prepareCO(request.readFromCache, request.putInCache))
  }

  implicit def toQueryHolder(request: SimpleSpecifiedIndexQueryMessage) = {
    new SpecifiedIndexQueryHolder(request.index, prepareNNQ(request.nnq), prepareBQ(request.bq), request.withMetadata, prepareQI(request.queryid), prepareCO(request.readFromCache, request.putInCache))
  }

  implicit def toQueryHolder(request: TimedQueryMessage) = {
    new TimedProgressiveQueryHolder(request.entity, prepareNNQ(request.nnq), prepareBQ(request.bq), Duration(request.time, TimeUnit.MILLISECONDS), request.withMetadata, prepareQI(request.queryid))
  }

  implicit def toQueryHolder(request: SimpleQueryMessage, onComplete: (ProgressiveQueryStatus.Value, DataFrame, VectorBase, String, Map[String, String]) => Unit) = new ProgressiveQueryHolder(request.entity, prepareNNQ(request.nnq), prepareBQ(request.bq), onComplete, request.withMetadata, prepareQI(request.queryid))

  implicit def toQueryHolder(request: CompoundQueryMessage) = {
    new CompoundQueryHolder(request.entity, prepareNNQ(request.nnq), request.indexFilterExpression, false, request.withMetadata, prepareQI(request.queryid))
  }

  implicit def toQueryHolder(request: SimpleBooleanQueryMessage) = {
    new BooleanQueryHolder(request.entity, prepareBQ(request.bq), prepareQI(request.queryid), prepareCO(request.readFromCache, request.putInCache))
  }

  implicit def toExpr(request: ExpressionQueryMessage): QueryExpression = {
    val order = request.order match {
      case ExpressionQueryMessage.OperationOrder.LEFTFIRST => ExpressionEvaluationOrder.LeftFirst
      case ExpressionQueryMessage.OperationOrder.RIGHTFIRST => ExpressionEvaluationOrder.RightFirst
      case ExpressionQueryMessage.OperationOrder.PARALLEL => ExpressionEvaluationOrder.Parallel
      case _ => null
    }

    val queryid = prepareQI(request.queryid)

    val operation = request.operation match {
      case ExpressionQueryMessage.Operation.UNION => UnionExpression(request.left, request.right, queryid)
      case ExpressionQueryMessage.Operation.INTERSECT => IntersectExpression(request.left, request.right, order, queryid)
      case ExpressionQueryMessage.Operation.EXCEPT => ExceptExpression(request.left, request.right, order, queryid)
      case _ => null //TODO: do we need a pre-filter option?
    }

    operation
  }

  implicit def toExpr(seqm: Option[SubExpressionQueryMessage]): QueryExpression = {
    if(seqm.isEmpty){
      return EmptyQueryExpression();
    }

    val expr = seqm.get.submessage match {
      case SubExpressionQueryMessage.Submessage.Eqm(x) => toExpr(x)
      case SubExpressionQueryMessage.Submessage.Ssiqm(request) => new SpecifiedIndexQueryHolder(request.index, prepareNNQ(request.nnq), prepareBQ(request.bq), request.withMetadata, Some(request.queryid))
      case SubExpressionQueryMessage.Submessage.Siqm(request) => new IndexQueryHolder(request.entity, IndexTypes.withIndextype(request.indextype).get, prepareNNQ(request.nnq), prepareBQ(request.bq), request.withMetadata, Some(request.queryid))
      case SubExpressionQueryMessage.Submessage.Ssqm(request) => new SequentialQueryHolder(request.entity, prepareNNQ(request.nnq), prepareBQ(request.bq), request.withMetadata, Some(request.queryid))
      case _ => EmptyQueryExpression();
    }

    expr
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

    val distance = prepareDistance(nnq.getDistance)

    NearestNeighbourQuery(nnq.query, distance, nnq.k, nnq.indexOnly, nnq.options)
  }

  private def prepareDistance(dm : DistanceMessage): DistanceFunction ={
    dm.distancetype match {
      case DistanceType.minkowski => {
        return NormBasedDistanceFunction(dm.options.get("norm").get.toDouble)
      }
      case _ => null
    }
  }

  /**
    *
    * @param option
    * @return
    */
  private def prepareBQ(option: Option[BooleanQueryMessage]): Option[BooleanQuery] = {
    if (option.isDefined) {
      val bq = option.get
      Some(BooleanQuery(Option(bq.where.map(bqm => (bqm.field, bqm.value))), Option(bq.joins.map(x => (x.table, x.columns))), Option(bq.prefilter.toSet)))
    } else {
      None
    }
  }

  private def prepareQI(queryid : String) = if(queryid != "" && queryid != null){
    Some(queryid)
  } else {
    None
  }

  private def prepareCO(readFromCache : Boolean, putInCache : Boolean) = Some(QueryCacheOptions(readFromCache, putInCache))
}


