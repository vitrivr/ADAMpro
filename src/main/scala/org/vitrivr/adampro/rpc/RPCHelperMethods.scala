package org.vitrivr.adampro.rpc

import java.util.concurrent.TimeUnit

import org.vitrivr.adampro.datatypes.AttributeTypes
import org.vitrivr.adampro.datatypes.AttributeTypes.{BOOLEANTYPE, DOUBLETYPE, FLOATTYPE, AttributeType, GEOGRAPHYTYPE, GEOMETRYTYPE, INTTYPE, LONGTYPE, SPARSEVECTORTYPE, STRINGTYPE, TEXTTYPE, VECTORTYPE}
import org.vitrivr.adampro.datatypes.vector.{SparseVectorWrapper, Vector}
import org.vitrivr.adampro.entity.AttributeDefinition
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.grpc.grpc.QueryMessage
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.{QueryCacheOptions, QueryHints}
import org.vitrivr.adampro.query.distance._
import org.vitrivr.adampro.query.handler.external.ExternalScanExpressions
import org.vitrivr.adampro.query.handler.generic.{QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.handler.internal.AggregationExpression._
import org.vitrivr.adampro.query.handler.internal.BooleanFilterExpression.BooleanFilterScanExpression
import org.vitrivr.adampro.query.handler.internal.ProjectionExpression._
import org.vitrivr.adampro.query.handler.internal._
import org.vitrivr.adampro.query.information.InformationLevels
import org.vitrivr.adampro.query.information.InformationLevels.{InformationLevel, LAST_STEP_ONLY}
import org.vitrivr.adampro.query.progressive.{QueryHintsProgressivePathChooser, SimpleProgressivePathChooser}
import org.vitrivr.adampro.query.query.{BooleanQuery, NearestNeighbourQuery, Predicate}
import org.vitrivr.adampro.utils.Logging
import org.vitrivr.adampro.datatypes.gis.GeographyWrapper
import org.vitrivr.adampro.grpc.grpc.DistanceMessage.DistanceType
import org.vitrivr.adampro.grpc.grpc._
import org.vitrivr.adampro.grpc._
import org.vitrivr.adampro.datatypes.vector.Vector._

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
private[rpc] object RPCHelperMethods extends Logging {

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
        new TimedScanExpression(entityname.get, nnq.get, preparePaths(qm.hints), Duration(time, TimeUnit.MILLISECONDS), queryid)(None)
      } else if (subexpression.isDefined) {
        new CompoundQueryExpression(toExpression(subexpression).get, queryid)
      } else if (entityname.isDefined) {
        HintBasedScanExpression(entityname.get, nnq, bq, hints, !qm.noFallback, queryid)(None)(ac)
      } else if (qm.from.get.source.isIndexes) {
        val indexes = qm.from.get.getIndexes.indexes
        new StochasticIndexQueryExpression(indexes.map(index => new IndexScanExpression(index)(nnq.get, queryid)(None)))(nnq.get, queryid)(None)
      } else if (indexname.isDefined) {
        var scan: Option[QueryExpression] = None

        if (bq.isDefined) {
          scan = Some(new BooleanFilterScanExpression(entityname.get)(bq.get, queryid)(scan))
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
        case ExpressionQueryMessage.Operation.JOIN => Success(IntersectExpression(left.get, right.get, order, Map(), queryid))
        case ExpressionQueryMessage.Operation.EXCEPT => Success(ExceptExpression(left.get, right.get, order, Map(), queryid))
        case _ => Failure(new GeneralAdamException("operation is unknown"))
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
        expr = ProjectionExpression(FieldNameProjection(attributes), expr, queryid)(ac)
      }

      if (!pm.op.isUnrecognized) {
        expr = pm.op match {
          case ProjectionMessage.Operation.COUNT => ProjectionExpression(CountOperationProjection(), expr, queryid)(ac)
          case ProjectionMessage.Operation.EXISTS => ProjectionExpression(ExistsOperationProjection(), expr, queryid)(ac)
          case ProjectionMessage.Operation.DISTINCT => ProjectionExpression(DistinctOperationProjection(), expr, queryid)(ac)
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
        prepareVector(nnq.query.get)
      } else {
        return Failure(new GeneralAdamException("no query specified"))
      }

      Success(NearestNeighbourQuery(nnq.attribute, fv, nnq.weights.map(prepareVector(_)), distance, nnq.k, nnq.indexOnly, nnq.options, partitions))
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    *
    * @param fv
    * @return
    */
  def prepareVector(fv: FeatureVectorMessage): MathVector = fv.feature match {
    case FeatureVectorMessage.Feature.DenseVector(request) => Vector.conv_draw2vec(request.vector.map(conv_float2vb))
    case FeatureVectorMessage.Feature.SparseVector(request) => Vector.conv_sraw2vec(request.position, request.vector.map(conv_float2vb), request.length)
    case FeatureVectorMessage.Feature.IntVector(request) => Vector.conv_draw2vec(request.vector.map(conv_int2vb)) //TODO: change to int vector
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
      case DistanceType.modulo => ModuloDistance
      case _ => {
        log.warn("no known distance function given, using Euclidean")
        NormBasedDistance(2)
      }
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
        bq.where.map(bqm => {
          val attribute = bqm.attribute
          val op = if (bqm.op.isEmpty) {
            None
          } else {
            Some(bqm.op)
          }
          val values = bqm.values.map(value => value.datatype.number match {
            case DataMessage.BOOLEANDATA_FIELD_NUMBER => value.getBooleanData
            case DataMessage.DOUBLEDATA_FIELD_NUMBER => value.getBooleanData
            case DataMessage.FLOATDATA_FIELD_NUMBER => value.getBooleanData
            case DataMessage.GEOGRAPHYDATA_FIELD_NUMBER => value.getGeographyData
            case DataMessage.GEOMETRYDATA_FIELD_NUMBER => value.getGeometryData
            case DataMessage.INTDATA_FIELD_NUMBER => value.getIntData
            case DataMessage.LONGDATA_FIELD_NUMBER => value.getLongData
            case DataMessage.STRINGDATA_FIELD_NUMBER => value.getStringData
            case _ => throw new GeneralAdamException("search predicates can not be of any type")
          })

          new Predicate(bqm.attribute, op, values)
        })
      } else {
        throw new GeneralAdamException("empty boolean query message given")
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
  def prepareAttributes(attributes: Seq[AttributeDefinitionMessage])(implicit ac: AdamContext): Seq[AttributeDefinition] = {
    attributes.map(attribute => {
      val attributetype = getAdamType(attribute.attributetype)

      if(attribute.handler != null && attribute.handler != ""){
        AttributeDefinition(attribute.name, attributetype, attribute.handler, attribute.params)
      } else {
        new AttributeDefinition(attribute.name, attributetype, attribute.params)
      }
    })
  }


  val grpc2adamTypes = Map(grpc.AttributeType.BOOLEAN -> AttributeTypes.BOOLEANTYPE, grpc.AttributeType.DOUBLE -> AttributeTypes.DOUBLETYPE, grpc.AttributeType.FLOAT -> AttributeTypes.FLOATTYPE,
    grpc.AttributeType.INT -> AttributeTypes.INTTYPE, grpc.AttributeType.LONG -> AttributeTypes.LONGTYPE, grpc.AttributeType.STRING -> AttributeTypes.STRINGTYPE,
    grpc.AttributeType.TEXT -> AttributeTypes.TEXTTYPE,
    grpc.AttributeType.VECTOR -> AttributeTypes.VECTORTYPE, grpc.AttributeType.SPARSEVECTOR -> AttributeTypes.SPARSEVECTORTYPE,
    grpc.AttributeType.GEOMETRY -> AttributeTypes.GEOMETRYTYPE, grpc.AttributeType.GEOGRAPHY -> AttributeTypes.GEOGRAPHYTYPE,
    grpc.AttributeType.AUTO -> AttributeTypes.AUTOTYPE)

  val adam2grpcTypes: Map[AttributeType, grpc.AttributeType] = grpc2adamTypes.map(_.swap)

  /**
    *
    * @param attributetype
    * @return
    */
  private[rpc] def getAdamType(attributetype: grpc.AttributeType) = grpc2adamTypes.getOrElse(attributetype, AttributeTypes.UNRECOGNIZEDTYPE)

  /**
    *
    * @param attributetype
    * @return
    */
  private[rpc] def getGrpcType(attributetype: AttributeType) = adam2grpcTypes.getOrElse(attributetype, grpc.AttributeType.UNKOWNAT)

  /**
    *
    * @param attributetype
    * @return
    */
  private[rpc] def prepareDataTypeConverter(attributetype : AttributeType): (DataMessage) => (Any) = attributetype match {
    case INTTYPE => (x) => x.getIntData
    case LONGTYPE => (x) => x.getLongData
    case FLOATTYPE => (x) => x.getFloatData
    case DOUBLETYPE => (x) => x.getDoubleData
    case STRINGTYPE => (x) => x.getStringData
    case TEXTTYPE => (x) => x.getStringData
    case BOOLEANTYPE => (x) => x.getBooleanData
    case VECTORTYPE => (x) =>  Vector.conv_vec2dspark(RPCHelperMethods.prepareVector(x.getFeatureData).asInstanceOf[DenseMathVector])
    case SPARSEVECTORTYPE => (x) => SparseVectorWrapper(RPCHelperMethods.prepareVector(x.getFeatureData).asInstanceOf[SparseMathVector]).toRow()
    case GEOGRAPHYTYPE => (x) => GeographyWrapper(x.getGeographyData).toRow()
    case GEOMETRYTYPE => (x) => GeographyWrapper(x.getGeometryData).toRow()
    case _ => throw new GeneralAdamException("field type " + attributetype.name + " not known")
  }
}


