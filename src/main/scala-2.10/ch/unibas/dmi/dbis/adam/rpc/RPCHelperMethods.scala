package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.http.grpc.DistanceMessage.DistanceType
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.CompoundQueryExpressions._
import ch.unibas.dmi.dbis.adam.query.datastructures.{QueryCacheOptions, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, NormBasedDistanceFunction}
import ch.unibas.dmi.dbis.adam.query.handler.QueryHandler._
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
private[rpc] object RPCHelperMethods {
  /* implicits */

  implicit def toQueryHolder(request: SimpleQueryMessage)(implicit ac: AdamContext) = {
    //TODO: possibly consider all query hints
    StandardQueryHolder(request.entity, QueryHints.withName(request.hints.head), Option(prepareNNQ(request.nnq)), prepareBQ(request.bq), None, prepareQI(request.queryid), prepareCO(request.readFromCache, request.putInCache))
  }

  implicit def toQueryHolder(request: SimpleSequentialQueryMessage)(implicit ac: AdamContext) = {
    new SequentialQueryHolder(request.entity, prepareNNQ(request.nnq), prepareBQ(request.bq), None, prepareQI(request.queryid), prepareCO(request.readFromCache, request.putInCache))
  }

  implicit def toQueryHolder(request: SimpleIndexQueryMessage)(implicit ac: AdamContext) = {
    new IndexQueryHolder(request.entity, IndexTypes.withIndextype(request.indextype).get, prepareNNQ(request.nnq), prepareBQ(request.bq), None, prepareQI(request.queryid), prepareCO(request.readFromCache, request.putInCache))
  }

  implicit def toQueryHolder(request: SimpleSpecifiedIndexQueryMessage)(implicit ac: AdamContext) = {
    new SpecifiedIndexQueryHolder(request.index, prepareNNQ(request.nnq), prepareBQ(request.bq), None, prepareQI(request.queryid), prepareCO(request.readFromCache, request.putInCache))
  }


  implicit def toQueryHolder(request: CompoundQueryMessage)(implicit ac: AdamContext) = {
    new CompoundQueryHolder(request.entity, prepareNNQ(request.nnq), request.indexFilterExpression, false, request.withMetadata, prepareQI(request.queryid))
  }

  implicit def toQueryHolder(request: SimpleBooleanQueryMessage)(implicit ac: AdamContext) = {
    new BooleanQueryHolder(request.entity, prepareBQ(request.bq), prepareQI(request.queryid), prepareCO(request.readFromCache, request.putInCache))
  }

  implicit def toExpr(request: ExpressionQueryMessage)(implicit ac: AdamContext): QueryExpression = {
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

  implicit def toExpr(seqm: Option[SubExpressionQueryMessage])(implicit ac: AdamContext): QueryExpression = {
    if (seqm.isEmpty) {
      return EmptyQueryExpression();
    }

    val expr = seqm.get.submessage match {
      case SubExpressionQueryMessage.Submessage.Eqm(x) => toExpr(x)
      case SubExpressionQueryMessage.Submessage.Ssiqm(request) => new SpecifiedIndexQueryHolder(request.index, prepareNNQ(request.nnq), prepareBQ(request.bq), None, Some(request.queryid), Some(QueryCacheOptions()))
      case SubExpressionQueryMessage.Submessage.Siqm(request) => new IndexQueryHolder(request.entity, IndexTypes.withIndextype(request.indextype).get, prepareNNQ(request.nnq), prepareBQ(request.bq), None, Some(request.queryid))
      case SubExpressionQueryMessage.Submessage.Ssqm(request) => new SequentialQueryHolder(request.entity, prepareNNQ(request.nnq), prepareBQ(request.bq), None, Some(request.queryid))
      case _ => EmptyQueryExpression();
    }

    expr
  }

  /**
    *
    * @param option
    * @return
    */
  def prepareNNQ(option: Option[NearestNeighbourQueryMessage]): NearestNeighbourQuery = {
    if (option.isEmpty) {
      throw new Exception("No kNN query specified.")
    }

    val nnq = option.get

    val distance = prepareDistance(nnq.getDistance)

    val partitions = if(!nnq.partitions.isEmpty){
      Some(nnq.partitions.toSet)
    } else {
      None
    }

    NearestNeighbourQuery(nnq.column, nnq.query.get.vector, distance, nnq.k, nnq.indexOnly, nnq.options, partitions)
  }

  def prepareDistance(dm: DistanceMessage): DistanceFunction = {
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
  def prepareBQ(option: Option[BooleanQueryMessage]): Option[BooleanQuery] = {
    if (option.isDefined) {
      val bq = option.get
      Some(BooleanQuery(Option(bq.where.map(bqm => (bqm.field, bqm.value))), Option(bq.joins.map(x => (x.table, x.columns)))))
    } else {
      None
    }
  }

  def prepareQI(queryid: String) = if (queryid != "" && queryid != null) {
    Some(queryid)
  } else {
    None
  }

  private def prepareCO(readFromCache: Boolean, putInCache: Boolean) = Some(QueryCacheOptions(readFromCache, putInCache))
}


