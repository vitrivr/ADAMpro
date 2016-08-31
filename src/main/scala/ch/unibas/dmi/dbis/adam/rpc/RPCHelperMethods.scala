package ch.unibas.dmi.dbis.adam.rpc

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapperUDT, FeatureVectorWrapper}
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.QueryCacheOptions
import ch.unibas.dmi.dbis.adam.query.distance._
import ch.unibas.dmi.dbis.adam.query.handler.external.ExternalScanExpressions
import ch.unibas.dmi.dbis.adam.query.handler.generic.{QueryEvaluationOptions, QueryExpression}
import ch.unibas.dmi.dbis.adam.query.handler.internal.AggregationExpression._
import ch.unibas.dmi.dbis.adam.query.handler.internal.BooleanFilterExpression.BooleanFilterScanExpression
import ch.unibas.dmi.dbis.adam.query.handler.internal.ProjectionExpression._
import ch.unibas.dmi.dbis.adam.query.handler.internal._
import ch.unibas.dmi.dbis.adam.query.information.InformationLevels
import ch.unibas.dmi.dbis.adam.query.information.InformationLevels.{InformationLevel, LAST_STEP_ONLY}
import ch.unibas.dmi.dbis.adam.query.progressive.{QueryHintsProgressivePathChooser, SimpleProgressivePathChooser}
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import org.apache.spark.sql.types
import org.apache.spark.sql.types.DataType
import org.vitrivr.adam.grpc.grpc.DistanceMessage.DistanceType
import org.vitrivr.adam.grpc.grpc._

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
    * @param qm
    * @return
    */
  implicit def toExpression(qm: QueryMessage)(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val queryid = prepareQueryId(qm.queryid)

      val entityname = qm.from.get.source.entity
      val indexname = qm.from.get.source.index
      val subexpression = qm.from.get.source.expression

      val bq = if (qm.bq.isDefined) {
        val prepared = prepareBQ(qm.bq.get)

        if (prepared.isFailure) {
          return Failure(prepared.failed.get)
        } else {
          Some(prepared.get)
        }
      } else {
        None
      }


      val nnq = if (qm.nnq.isDefined) {
        val prepared = prepareNNQ(qm.nnq.get)

        if (prepared.isFailure) {
          return Failure(prepared.failed.get)
        } else {
          Some(prepared.get)
        }
      } else {
        None
      }


      val hints = QueryHints.withName(qm.hints)

      val time = qm.time

      //TODO: use cache options
      val cacheOptions = prepareCacheExpression(qm.readFromCache, qm.putInCache)

      var scan: QueryExpression = null

      //selection
      scan = if (time > 0) {
        new TimedScanExpression(entityname.get, nnq.get, preparePaths(qm.hints), Duration(time, TimeUnit.MILLISECONDS), queryid)()
      } else if (subexpression.isDefined) {
        new CompoundQueryExpression(toExpression(subexpression).get, queryid)
      } else if (entityname.isDefined) {
        HintBasedScanExpression(entityname.get, nnq, bq, hints, !qm.noFallback, queryid)()
      } else if (qm.from.get.source.isIndexes) {
        val indexes = qm.from.get.getIndexes.indexes
        new StochasticIndexQueryExpression(indexes.map(index => new IndexScanExpression(index)(nnq.get, queryid)()))(nnq.get, queryid)()
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
      if (qm.projection.isDefined) {
        val projection = prepareProjectionExpression(qm.projection.get, scan, queryid)

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
    * @param ehqm
    * @return
    */
  implicit def toExpression(ehqm: ExternalHandlerQueryMessage)(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val handler = ehqm.handler
      val entityname = ehqm.entity
      val params = ehqm.params
      val queryid = prepareQueryId(ehqm.queryid)

      Success(ExternalScanExpressions.toQueryExpression(handler, entityname, ehqm.params, queryid))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    *
    * @param eqm
    * @return
    */
  implicit def toExpression(eqm: ExpressionQueryMessage)(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val order = eqm.order match {
        case ExpressionQueryMessage.OperationOrder.LEFTFIRST => ExpressionEvaluationOrder.LeftFirst
        case ExpressionQueryMessage.OperationOrder.RIGHTFIRST => ExpressionEvaluationOrder.RightFirst
        case ExpressionQueryMessage.OperationOrder.PARALLEL => ExpressionEvaluationOrder.Parallel
        case _ => null
      }

      val queryid = prepareQueryId(eqm.queryid)

      val left = toExpression(eqm.left)
      if (left.isFailure) {
        return Failure(left.failed.get)
      }

      val right = toExpression(eqm.right)
      if (right.isFailure) {
        return Failure(right.failed.get)
      }

      //TODO: possibly add options to aggregation operations

      eqm.operation match {
        case ExpressionQueryMessage.Operation.UNION => Success(UnionExpression(left.get, right.get, Map(), queryid))
        case ExpressionQueryMessage.Operation.INTERSECT => Success(IntersectExpression(left.get, right.get, order, Map(), queryid))
        case ExpressionQueryMessage.Operation.EXCEPT => Success(ExceptExpression(left.get, right.get, order, Map(), queryid))
        case _ => Failure(new Exception("operation unknown"))
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
    * @param qm
    */
  def prepareEvaluationOptions(qm: QueryMessage): Option[QueryEvaluationOptions] = {
    //source provenance option
    val storeSourceProvenance = prepareInformationLevel(qm.information).contains(InformationLevels.SOURCE_PROVENANCE)

    Some(QueryEvaluationOptions(storeSourceProvenance))
  }


  /**
    *
    * @param pm
    * @return
    */
  def prepareProjectionExpression(pm: ProjectionMessage, qe: QueryExpression, queryid: Option[String])(implicit ac: AdamContext): Try[QueryExpression] = {
    try {
      val attributes = pm.getAttributes.attribute

      var expr = qe

      if (attributes.nonEmpty) {
        expr = ProjectionExpression(FieldNameProjection(attributes), expr, queryid)
      }

      if (!pm.op.isUnrecognized) {
        expr = pm.op match {
          case ProjectionMessage.Operation.COUNT => ProjectionExpression(CountOperationProjection(), expr, queryid)
          case ProjectionMessage.Operation.EXISTS => ProjectionExpression(ExistsOperationProjection(), expr, queryid)
          case ProjectionMessage.Operation.DISTINCT => ProjectionExpression(DistinctOperationProjection(), expr, queryid)
          case _ => expr
        }
      }

      Success(expr)
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

      val fv = if (nnq.query.isDefined) {
        prepareFeatureVector(nnq.query.get)
      } else {
        return Failure(new GeneralAdamException("no query specified"))
      }

      Success(NearestNeighbourQuery(nnq.attribute, fv, nnq.weights.map(prepareFeatureVector(_)), distance, nnq.k, nnq.indexOnly, nnq.options, partitions))
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
    case FeatureVectorMessage.Feature.DenseVector(request) => FeatureVectorWrapper(request.vector).vector
    case FeatureVectorMessage.Feature.SparseVector(request) => new FeatureVectorWrapper(request.position, request.vector, request.length).vector
    case FeatureVectorMessage.Feature.IntVector(request) => FeatureVectorWrapper(request.vector.map(_.toFloat)).vector //TODO: change to int vector
    case _ => null
  }


  /**
    *
    * @param dm
    * @return
    */
  def prepareDistance(dm: Option[DistanceMessage]): DistanceFunction = {
    if (dm.isEmpty) {
      return NormBasedDistance(2)
    }

    dm.get.distancetype match {
      case DistanceType.chisquared => ChiSquaredDistance
      case DistanceType.correlation => CorrelationDistance
      case DistanceType.cosine => CosineDistance
      case DistanceType.hamming => HammingDistance
      case DistanceType.jaccard => JaccardDistance
      case DistanceType.kullbackleibler => KullbackLeiblerDivergence
      case DistanceType.chebyshev => ChebyshevDistance
      case DistanceType.euclidean => EuclideanDistance
      case DistanceType.squaredeuclidean => SquaredEuclideanDistance
      case DistanceType.manhattan => ManhattanDistance
      case DistanceType.minkowski => {
        NormBasedDistance(dm.get.options.get("norm").get.toDouble)
      }
      case DistanceType.spannorm => SpanNormDistance
      case _ => NormBasedDistance(2)
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
        Some(bq.where.map(bqm => (bqm.attribute, bqm.value)))
      } else {
        None
      }
      Success(BooleanQuery(where))
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
    * @param ilm
    * @return
    */
  def prepareInformationLevel(ilm: Seq[QueryMessage.InformationLevel]): Seq[InformationLevel] = {
    val levels = ilm.map { level =>
      level match {
        case QueryMessage.InformationLevel.INFORMATION_FULL_TREE => InformationLevels.FULL_TREE
        case QueryMessage.InformationLevel.INFORMATION_LAST_STEP_ONLY => InformationLevels.LAST_STEP_ONLY
        case QueryMessage.InformationLevel.INFORMATION_INTERMEDIATE_RESULTS => InformationLevels.INTERMEDIATE_RESULTS
        case QueryMessage.InformationLevel.WITH_PROVENANCE_PARTITION_INFORMATION => InformationLevels.PARTITION_PROVENANCE
        case QueryMessage.InformationLevel.WITH_PROVENANCE_SOURCE_INFORMATION => InformationLevels.SOURCE_PROVENANCE
        case _ => null
      }
    }.filterNot(_ == null)

    if (levels.isEmpty) {
      Seq(LAST_STEP_ONLY)
    } else {
      levels
    }
  }

  /**
    *
    */
  def prepareAttributes(attributes: Seq[AttributeDefinitionMessage]): Seq[AttributeDefinition] = {
    attributes.map(attribute => {
      val handler = attribute.handler match {
        case HandlerType.RELATIONAL => Some("relational")
        case HandlerType.FILE => Some("file")
        case HandlerType.SOLR => Some("solr")
        case _ => None
      }

      AttributeDefinition(attribute.name, matchFields(attribute.attributetype), attribute.pk, attribute.unique, attribute.indexed, handler)
    })
  }

  /**
    *
    * @param ft
    * @return
    */
  private def matchFields(ft: AttributeType) = ft match {
    case AttributeType.BOOLEAN => FieldTypes.BOOLEANTYPE
    case AttributeType.DOUBLE => FieldTypes.DOUBLETYPE
    case AttributeType.FLOAT => FieldTypes.FLOATTYPE
    case AttributeType.INT => FieldTypes.INTTYPE
    case AttributeType.LONG => FieldTypes.LONGTYPE
    case AttributeType.STRING => FieldTypes.STRINGTYPE
    case AttributeType.TEXT => FieldTypes.TEXTTYPE
    case AttributeType.FEATURE => FieldTypes.FEATURETYPE
    case _ => FieldTypes.UNRECOGNIZEDTYPE
  }

  /**
    *
    * @param datatype
    * @return
    */
  def prepareDataTypeConverter(datatype: DataType): (DataMessage) => (Any) = datatype match {
    case types.BooleanType => (x) => x.getBooleanData
    case types.DoubleType => (x) => x.getBooleanData
    case types.FloatType => (x) => x.getFloatData
    case types.IntegerType => (x) => x.getIntData
    case types.LongType => (x) => x.getLongData
    case types.StringType => (x) => x.getStringData
    case _: FeatureVectorWrapperUDT => (x) => FeatureVectorWrapper(RPCHelperMethods.prepareFeatureVector(x.getFeatureData))
  }
}


