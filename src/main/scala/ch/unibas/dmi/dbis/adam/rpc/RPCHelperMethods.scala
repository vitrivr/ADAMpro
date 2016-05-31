package ch.unibas.dmi.dbis.adam.rpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.http.grpc.DistanceMessage.DistanceType
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.QueryExpression
import ch.unibas.dmi.dbis.adam.query.handler.internal.BooleanFilterExpression.BooleanFilterScanExpression
import ch.unibas.dmi.dbis.adam.query.handler.internal.ProjectionExpression.{ExistsOperationProjection, CountOperationProjection, FieldNameProjection, ProjectionField}
import ch.unibas.dmi.dbis.adam.query.handler.internal._
import AggregationExpression._
import ch.unibas.dmi.dbis.adam.query.datastructures.QueryCacheOptions
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, NormBasedDistanceFunction}
import ch.unibas.dmi.dbis.adam.query.handler.external.ExternalScanExpressions
import ch.unibas.dmi.dbis.adam.query.handler.internal._
import ch.unibas.dmi.dbis.adam.query.progressive.{QueryHintsProgressivePathChooser, SimpleProgressivePathChooser}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
private[rpc] object RPCHelperMethods {


  implicit def toExpression(request: QueryMessage)(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val queryid = prepareQueryId(request.queryid)

      val entityname = request.from.get.source.entity
      val indexname = request.from.get.source.index
      val subexpression = request.from.get.source.expression

      val bq = prepareBQ(request.bq)
      val nnq = prepareNNQ(request.nnq)

      val hints = QueryHints.withName(request.hints)

      val time = request.time

      //TODO: add cache options
      val cacheOptions = prepareCacheExpression(request.readFromCache, request.putInCache)



      var scan: QueryExpression = null

      scan = if (time > 0) {
        new TimedScanExpression(entityname.get, nnq.get, preparePaths(request.hints), Duration(time, TimeUnit.MILLISECONDS), queryid)()
      } else if (subexpression.isDefined) {
        new CompoundQueryExpression(toExpression(subexpression).get, queryid)
      } else if (entityname.isDefined) {
        HintBasedScanExpression(entityname.get, nnq, bq, hints, queryid)()
      } else if (indexname.isDefined) {

        var scan: Option[QueryExpression] = None

        if (bq.isDefined) {
          scan = Some(new BooleanFilterScanExpression(entityname.get)(bq.get)(scan))
        }

        if (nnq.isDefined) {
          scan = Some(new IndexScanExpression(indexname.get)(nnq.get, queryid)(scan))
        }

        scan.get
      } else {
        null
      }

      scan = prepareProjectionExpression(request.projection, scan, queryid).getOrElse(scan)

      Success(scan)
    } catch {
      case e: Exception => Failure(e)
    }
  }


  implicit def toExpression(request: ExternalHandlerQueryMessage)(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val handler = request.handler
      val entityname = request.entity
      val params = request.params
      val queryid = prepareQueryId(request.queryid)

      Success(ExternalScanExpressions.toQueryExpression(handler, entityname, request.params, queryid))
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

      val queryid = prepareQueryId(request.queryid)

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
        return Success(EmptyExpression())
      }

      seqm.get.submessage match {
        case SubExpressionQueryMessage.Submessage.Eqm(request) => toExpression(request)
        case SubExpressionQueryMessage.Submessage.Qm(request) => toExpression(request)
        case SubExpressionQueryMessage.Submessage.Ehqm(request) => toExpression(request)
        case _ => Success(EmptyExpression())
      }
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    *
    * @param pm
    * @param ac
    * @return
    */
  def prepareProjectionExpression(pm: Option[ProjectionMessage], qe: QueryExpression, queryid : Option[String])(implicit ac: AdamContext): Option[QueryExpression] = {
    val projection: Option[ProjectionField] = if (pm.isEmpty) {
      None
    } else if (pm.get.submessage.isField) {
      val fields = pm.get.getField.field

      if (fields.isEmpty) {
        None
      } else {
        Some(FieldNameProjection(fields))
      }
    } else if (pm.get.submessage.isOp) {
      pm.get.getOp match {
        case ProjectionMessage.Operation.COUNT => Some(CountOperationProjection())
        case ProjectionMessage.Operation.EXISTS => Some(ExistsOperationProjection())
        case _ => None
      }
    } else {
      None
    }

    if (projection.isDefined) {
      Some(ProjectionExpression(projection.get, qe, queryid))
    } else {
      None
    }
  }


  /**
    *
    * @param option
    * @return
    */
  def prepareNNQ(option: Option[NearestNeighbourQueryMessage]): Option[NearestNeighbourQuery] = {
    if (option.isEmpty) {
      return None
    }

    val nnq = option.get

    val distance = prepareDistance(nnq.getDistance)

    val partitions = if (!nnq.partitions.isEmpty) {
      Some(nnq.partitions.toSet)
    } else {
      None
    }

    val fv = prepareFeatureVector(nnq.query.get)

    Some(NearestNeighbourQuery(nnq.column, fv, nnq.weights.map(prepareFeatureVector(_)), distance, nnq.k, nnq.indexOnly, nnq.options, partitions))
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
        NormBasedDistanceFunction(dm.options.get("norm").get.toDouble)
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
      val where = if (!bq.where.isEmpty) {
        Some(bq.where.map(bqm => (bqm.field, bqm.value)))
      } else {
        None
      }
      val joins = if (!bq.joins.isEmpty) {
        Some(bq.joins.map(x => (x.table, x.columns)))
      } else {
        None
      }
      Some(BooleanQuery(where, joins))
    } else {
      None
    }
  }


  def prepareQueryId(queryid: String) = if (queryid != "" && queryid != null) {
    Some(queryid)
  } else {
    None
  }

  def preparePaths(hints: Seq[String])(implicit ac: AdamContext) = if (hints.isEmpty) {
    new SimpleProgressivePathChooser()
  } else {
    new QueryHintsProgressivePathChooser(hints.map(QueryHints.withName(_).get))
  }

  private def prepareCacheExpression(readFromCache: Boolean, putInCache: Boolean) = Some(QueryCacheOptions(readFromCache, putInCache))
}


