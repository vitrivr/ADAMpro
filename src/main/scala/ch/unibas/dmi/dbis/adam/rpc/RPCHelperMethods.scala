package ch.unibas.dmi.dbis.adam.rpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.http.grpc.DistanceMessage.DistanceType
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.QueryCacheOptions
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, NormBasedDistanceFunction}
import ch.unibas.dmi.dbis.adam.query.handler.external.ExternalScanExpressions
import ch.unibas.dmi.dbis.adam.query.handler.generic.QueryExpression
import ch.unibas.dmi.dbis.adam.query.handler.internal.AggregationExpression._
import ch.unibas.dmi.dbis.adam.query.handler.internal.BooleanFilterExpression.BooleanFilterScanExpression
import ch.unibas.dmi.dbis.adam.query.handler.internal.ProjectionExpression.{CountOperationProjection, ExistsOperationProjection, FieldNameProjection, ProjectionField}
import ch.unibas.dmi.dbis.adam.query.handler.internal._
import ch.unibas.dmi.dbis.adam.query.information.InformationLevels
import ch.unibas.dmi.dbis.adam.query.information.InformationLevels.{InformationLevel, LAST_STEP_ONLY}
import ch.unibas.dmi.dbis.adam.query.progressive.{QueryHintsProgressivePathChooser, SimpleProgressivePathChooser}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import spire.std.option

import scala.concurrent.duration.Duration
import scala.util.{Success, Failure, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
private[rpc] object RPCHelperMethods {

  /**
    *
    * @param request
    * @return
    */
  implicit def toExpression(request: QueryMessage)(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val queryid = prepareQueryId(request.queryid)

      val entityname = request.from.get.source.entity
      val indexname = request.from.get.source.index
      val subexpression = request.from.get.source.expression

      val bq = if(request.bq.isDefined){
        val prepared = prepareBQ(request.bq.get)

        if (prepared.isFailure) {
          return Failure(prepared.failed.get)
        } else {
          Some(prepared.get)
        }
      } else {
        None
      }


      val nnq = if (request.nnq.isDefined) {
        val prepared = prepareNNQ(request.nnq.get)

        if (prepared.isFailure) {
          return Failure(prepared.failed.get)
        } else {
          Some(prepared.get)
        }
      } else {
        None
      }


      val hints = QueryHints.withName(request.hints)

      val time = request.time

      //TODO: add cache options
      val cacheOptions = prepareCacheExpression(request.readFromCache, request.putInCache)

      var scan: QueryExpression = null

      //selection
      scan = if (time > 0) {
        new TimedScanExpression(entityname.get, nnq.get, preparePaths(request.hints), Duration(time, TimeUnit.MILLISECONDS), queryid)()
      } else if (subexpression.isDefined) {
        new CompoundQueryExpression(toExpression(subexpression).get, queryid)
      } else if (entityname.isDefined) {
        HintBasedScanExpression(entityname.get, nnq, bq, hints, true, queryid)()
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

      //projection
      if (request.projection.isDefined) {
        val projection = prepareProjectionExpression(request.projection.get, scan, queryid)

        if (projection.isSuccess) {
          scan = projection.get
        } else {
          return projection
        }
      }

      Success(scan)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    *
    * @param request
    * @return
    */
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

  /**
    *
    * @param request
    * @return
    */
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

  /**
    *
    * @param seqm
    * @return
    */
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
    * @return
    */
  def prepareProjectionExpression(pm: ProjectionMessage, qe: QueryExpression, queryid: Option[String])(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      if (pm.submessage.isOp) {
        pm.getOp match {
          case ProjectionMessage.Operation.COUNT => Success(ProjectionExpression(CountOperationProjection(), qe, queryid))
          case ProjectionMessage.Operation.EXISTS => Success(ProjectionExpression(ExistsOperationProjection(), qe, queryid))
          case _ => Success(qe)
        }
      } else {
        val fields = pm.getField.field

        if (fields.isEmpty) {
          Success(qe)
        } else {
          Success(ProjectionExpression(FieldNameProjection(fields), qe, queryid))
        }
      }
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    *
    * @param nnq
    * @return
    */
  def prepareNNQ(nnq: NearestNeighbourQueryMessage): Try[NearestNeighbourQuery] = {
    try {
      val distance = prepareDistance(nnq.distance)

      val partitions = if (!nnq.partitions.isEmpty) {
        Some(nnq.partitions.toSet)
      } else {
        None
      }

      val fv = if(nnq.query.isDefined){
        prepareFeatureVector(nnq.query.get)
      } else {
        return Failure(new GeneralAdamException("no query specified"))
      }

      Success(NearestNeighbourQuery(nnq.column, fv, nnq.weights.map(prepareFeatureVector(_)), distance, nnq.k, nnq.indexOnly, nnq.options, partitions))
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    *
    * @param fv
    * @return
    */
  def prepareFeatureVector(fv: FeatureVectorMessage): FeatureVector = fv.feature match {
    //TODO: adjust here to use sparse vector too
    case FeatureVectorMessage.Feature.DenseVector(request) => FeatureVectorWrapper(request.vector).vector
    case FeatureVectorMessage.Feature.SparseVector(request) => new FeatureVectorWrapper(request.position, request.vector, request.length).vector
    case FeatureVectorMessage.Feature.IntVector(request) => FeatureVectorWrapper(request.vector.map(_.toFloat)).vector //TODO: change in future to int vector
    case _ => null
  }


  /**
    *
    * @param dm
    * @return
    */
  def prepareDistance(dm: Option[DistanceMessage]): DistanceFunction = {
    if(dm.isEmpty){
      return NormBasedDistanceFunction(2)
    }

    dm.get.distancetype match {
      case DistanceType.minkowski => {
        NormBasedDistanceFunction(dm.get.options.get("norm").get.toDouble)
      }
      case _ => NormBasedDistanceFunction(2)
    }
  }

  /**
    *
    * @param bq
    * @return
    */
  def prepareBQ(bq: BooleanQueryMessage): Try[BooleanQuery] = {
    try {
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
      Success(BooleanQuery(where, joins))
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    *
    * @param queryid
    * @return
    */
  def prepareQueryId(queryid: String) = if (queryid != "" && queryid != null) {
    Some(queryid)
  } else {
    None
  }

  /**
    *
    * @param hints
    * @return
    */
  def preparePaths(hints: Seq[String])(implicit ac: AdamContext) = if (hints.isEmpty) {
    new SimpleProgressivePathChooser()
  } else {
    new QueryHintsProgressivePathChooser(hints.map(QueryHints.withName(_).get))
  }

  /**
    *
    * @param readFromCache
    * @param putInCache
    * @return
    */
  private def prepareCacheExpression(readFromCache: Boolean, putInCache: Boolean) = Some(QueryCacheOptions(readFromCache, putInCache))


  /**
    *
    * @param message
    * @return
    */
  def prepareInformationLevel(message: Seq[QueryMessage.InformationLevel]): Seq[InformationLevel] = {
    val levels = message.map { level =>
      level match {
        case QueryMessage.InformationLevel.INFORMATION_FULL_TREE => InformationLevels.FULL_TREE
        case QueryMessage.InformationLevel.INFORMATION_LAST_STEP_ONLY => InformationLevels.LAST_STEP_ONLY
        case QueryMessage.InformationLevel.INFORMATION_INTERMEDIATE_RESULTS => InformationLevels.INTERMEDIATE_RESULTS
        case _ => null
      }
    }.filterNot(_ == null)

    if (levels.isEmpty) {
      Seq(LAST_STEP_ONLY)
    } else {
      levels
    }
  }
}


