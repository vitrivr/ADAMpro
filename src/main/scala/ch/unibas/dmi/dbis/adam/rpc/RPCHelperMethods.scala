package ch.unibas.dmi.dbis.adam.rpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.http.grpc.DistanceMessage.DistanceType
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.datastructures.CompoundQueryExpressions._
import ch.unibas.dmi.dbis.adam.query.datastructures.{QueryCacheOptions, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, NormBasedDistanceFunction}
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints
import ch.unibas.dmi.dbis.adam.query.handler.external.ExternalHandlers
import ch.unibas.dmi.dbis.adam.query.handler.internal._
import ch.unibas.dmi.dbis.adam.query.progressive.{QueryHintsProgressivePathChooser, SimpleProgressivePathChooser}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

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
      val queryid = prepareQI(request.queryid)
      val projection = preparePJ(request.projection)
      val entityname = request.from.get.submessage.entity
      val indexname = request.from.get.submessage.index
      val subexpression = request.from.get.submessage.expression
      val bq = prepareBQ(request.bq)
      val nnq = prepareNNQ(request.nnq)
      val time = request.time
      val tiq = None
      val cacheOptions = prepareCO(request.readFromCache, request.putInCache)

      if (time > 0) {
        Success(new TimedProgressiveQueryHolder(entityname.get, nnq.get, bq, tiq, preparePaths(request.hints), Duration(time, TimeUnit.MILLISECONDS), false, queryid, cacheOptions))
      } else if (subexpression.isDefined){
        Success(new CompoundQueryHolder(toExpression(subexpression).get, queryid))
      } else if (nnq.isEmpty && bq.isDefined) {
        Success(new BooleanQueryHolder(entityname.get)(bq, queryid, cacheOptions))
      } else if (indexname.isDefined) {
        Success(new IndexQueryHolder(indexname.get)(nnq.get, bq, tiq, queryid, cacheOptions))
      } else {
        Success(StandardQueryHolder(entityname.get)(QueryHints.withName(request.hints), nnq, bq, None, queryid, cacheOptions))
      }
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
        case SubExpressionQueryMessage.Submessage.Qm(request) => toExpression(request)
        case SubExpressionQueryMessage.Submessage.Ehqm(request) => toExpression(request)
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
    if(option.isEmpty){
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

  def preparePJ(pm: Option[ProjectionMessage])(implicit ac: AdamContext): (DataFrame => DataFrame) = {
    if (pm.isEmpty) {
      return (df: DataFrame) => df
    }

    if (pm.get.submessage.isField) {
      val fields = pm.get.getField.field

      if (fields.isEmpty) {
        return (df: DataFrame) => df
      } else {
        import org.apache.spark.sql.functions.col
        (df: DataFrame) => (df.select(fields.map(col(_)): _*))
      }
    }

    if (pm.get.submessage.isOp) {
      pm.get.getOp match {
        case ProjectionMessage.Operation.COUNT => return (df: DataFrame) => ac.sqlContext.createDataFrame(ac.sc.makeRDD(Seq(Row(df.count()))), StructType(Seq(StructField("count", LongType))))
        case ProjectionMessage.Operation.EXISTS => return (df: DataFrame) => ac.sqlContext.createDataFrame(ac.sc.makeRDD(Seq(Row(df.count() > 1))), StructType(Seq(StructField("exists", BooleanType))))
        case _ => return (df: DataFrame) => df
      }
    }

    (df: DataFrame) => df
  }

  def prepareQI(queryid: String) = if (queryid != "" && queryid != null) {
    Some(queryid)
  } else {
    None
  }

  def preparePaths(hints: Seq[String])(implicit ac: AdamContext) = if (hints.isEmpty) {
    new SimpleProgressivePathChooser()
  } else {
    new QueryHintsProgressivePathChooser(hints.map(QueryHints.withName(_).get))
  }

  private def prepareCO(readFromCache: Boolean, putInCache: Boolean) = Some(QueryCacheOptions(readFromCache, putInCache))
}


