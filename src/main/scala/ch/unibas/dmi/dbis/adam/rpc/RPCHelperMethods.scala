package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.http.grpc.DistanceMessage.DistanceType
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.CompoundQueryExpressions._
import ch.unibas.dmi.dbis.adam.query.datastructures.{QueryCacheOptions, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, NormBasedDistanceFunction}
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints
import ch.unibas.dmi.dbis.adam.query.handler.external.ExternalHandlers
import ch.unibas.dmi.dbis.adam.query.handler.internal._
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}

import scala.util.{Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
private[rpc] object RPCHelperMethods {


  implicit def toExpression(request: SimpleQueryMessage) : Try[QueryExpression] = {
    try {
      val entityname = request.entity
      val hints = QueryHints.withName(request.hints)
      val nnq = prepareNNQ(request.nnq)
      val bq = prepareBQ(request.bq)
      val tiq = None
      val queryid = prepareQI(request.queryid)
      val cacheOptions = prepareCO(request.readFromCache, request.putInCache)

      Success(StandardQueryHolder(entityname)(hints, nnq, bq, None, queryid, cacheOptions))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  implicit def toExpression(request: SimpleSequentialQueryMessage)(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val entityname = request.entity
      val nnq = prepareNNQ(request.nnq)
      val bq = prepareBQ(request.bq)
      val tiq = None
      val queryid = prepareQI(request.queryid)
      val cacheOptions = prepareCO(request.readFromCache, request.putInCache)

      Success(SequentialQueryHolder(entityname)(nnq.get, bq, None, queryid, cacheOptions))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  implicit def toExpression(request: SimpleIndexQueryMessage)(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val entityname = request.entity
      val indextype = IndexTypes.withIndextype(request.indextype)
      val nnq = prepareNNQ(request.nnq)
      val bq = prepareBQ(request.bq)
      val tiq = None
      val queryid = prepareQI(request.queryid)
      val cacheOptions = prepareCO(request.readFromCache, request.putInCache)

      Success(new IndexQueryHolder(entityname, indextype.get)(nnq.get, bq, None, queryid, cacheOptions))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  implicit def toExpression(request: SimpleSpecifiedIndexQueryMessage)(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val indexname = request.index
      val nnq = prepareNNQ(request.nnq)
      val bq = prepareBQ(request.bq)
      val tiq = None
      val queryid = prepareQI(request.queryid)
      val cacheOptions = prepareCO(request.readFromCache, request.putInCache)

      Success(new IndexQueryHolder(indexname)(nnq.get, bq, None, queryid, cacheOptions))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  implicit def toExpression(request: ExternalHandlerQueryMessage)(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val handler = request.handler
      val entityname = request.entity
      val params = request.params
      val queryid = prepareQI(request.queryid)

      Success(ExternalHandlers.toQueryExpression(handler, entityname, request.params, queryid))
    } catch {
      case e: Exception => Failure(e)
    }
  }


  implicit def toExpression(request: CompoundQueryMessage)(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val entityname = request.entity
      val subexpression = toExpression(request.indexFilterExpression)

      if(subexpression.isFailure){
        return subexpression
      }

      val queryid = prepareQI(request.queryid)

      Success(new CompoundQueryHolder(entityname)(subexpression.get, queryid))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  implicit def toExpression(request: SimpleBooleanQueryMessage)(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val entityname = request.entity
      val bq = prepareBQ(request.bq)
      val queryid = prepareQI(request.queryid)
      val cacheOptions = prepareCO(request.readFromCache, request.putInCache)

      Success(new BooleanQueryHolder(entityname)(bq, queryid, cacheOptions))

    } catch {
      case e: Exception => Failure(e)
    }
  }

  implicit def toExpression(request: ExpressionQueryMessage)(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val order = request.order match {
        case ExpressionQueryMessage.OperationOrder.LEFTFIRST => ExpressionEvaluationOrder.LeftFirst
        case ExpressionQueryMessage.OperationOrder.RIGHTFIRST => ExpressionEvaluationOrder.RightFirst
        case ExpressionQueryMessage.OperationOrder.PARALLEL => ExpressionEvaluationOrder.Parallel
        case _ => null
      }

      val queryid = prepareQI(request.queryid)

      val left = toExpression(request.left)
      if (left.isFailure) {
        return Failure(left.failed.get)
      }

      val right = toExpression(request.right)
      if (right.isFailure) {
        return Failure(right.failed.get)
      }

      request.operation match {
        case ExpressionQueryMessage.Operation.UNION => Success(UnionExpression(left.get, right.get, queryid))
        case ExpressionQueryMessage.Operation.INTERSECT => Success(IntersectExpression(left.get, right.get, order, queryid))
        case ExpressionQueryMessage.Operation.EXCEPT => Success(ExceptExpression(left.get, right.get, order, queryid))
        case _ => Failure(new Exception("operation unknown")) //TODO: do we need a pre-filter option?
      }
    } catch {
      case e: Exception => Failure(e)
    }
  }

  implicit def toExpression(seqm: Option[SubExpressionQueryMessage])(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      if (seqm.isEmpty) {
        return Success(EmptyQueryExpression())
      }

      seqm.get.submessage match {
        case SubExpressionQueryMessage.Submessage.Eqm(request) => toExpression(request)
        case SubExpressionQueryMessage.Submessage.Ssiqm(request) => toExpression(request)
        case SubExpressionQueryMessage.Submessage.Siqm(request) => toExpression(request)
        case SubExpressionQueryMessage.Submessage.Ssqm(request) => toExpression(request)
        case SubExpressionQueryMessage.Submessage.Ehqm(request) => toExpression(request)
        case SubExpressionQueryMessage.Submessage.Sbqm(request) => toExpression(request)
        case _ => Success(EmptyQueryExpression())
      }
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    *
    * @param option
    * @return
    */
  def prepareNNQ(option: Option[NearestNeighbourQueryMessage]): Option[NearestNeighbourQuery] = {
    if (option.isEmpty) {
      throw new Exception("No kNN query specified.")
    }

    val nnq = option.get

    val distance = prepareDistance(nnq.getDistance)

    val partitions = if (!nnq.partitions.isEmpty) {
      Some(nnq.partitions.toSet)
    } else {
      None
    }

    val fv = prepareFeatureVector(nnq.query.get)

    Some(NearestNeighbourQuery(nnq.column, fv, distance, nnq.k, nnq.indexOnly, nnq.options, partitions))
  }

  def prepareFeatureVector(fv: FeatureVectorMessage): FeatureVector = fv.feature match {
    //TODO: adjust here to use sparse vector too
    case FeatureVectorMessage.Feature.DenseVector(request) => FeatureVectorWrapper(request.vector).vector
    case FeatureVectorMessage.Feature.SparseVector(request) => new FeatureVectorWrapper(request.position, request.vector, request.length).vector
    case FeatureVectorMessage.Feature.IntVector(request) => FeatureVectorWrapper(request.vector.map(_.toFloat)).vector //TODO: change in future to int vector
    case _ => null
  }


  def prepareDistance(dm: DistanceMessage): DistanceFunction = {
    dm.distancetype match {
      case DistanceType.minkowski => {
        return NormBasedDistanceFunction(dm.options.get("norm").get.toDouble)
      }
      case _ => NormBasedDistanceFunction(2)
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


