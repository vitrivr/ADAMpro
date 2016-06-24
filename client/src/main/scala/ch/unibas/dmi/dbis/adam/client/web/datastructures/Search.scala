package ch.unibas.dmi.dbis.adam.client.web.datastructures

import ch.unibas.dmi.dbis.adam.client.web.ProgressiveQueryStatus
import ch.unibas.dmi.dbis.adam.http.grpc.BooleanQueryMessage.WhereMessage
import ch.unibas.dmi.dbis.adam.http.grpc.DataMessage.Datatype
import ch.unibas.dmi.dbis.adam.http.grpc.QueryMessage.InformationLevel
import ch.unibas.dmi.dbis.adam.http.grpc.QueryMessage.InformationLevel.{INFORMATION_FULL_TREE, INFORMATION_INTERMEDIATE_RESULTS, INFORMATION_LAST_STEP_ONLY}
import ch.unibas.dmi.dbis.adam.http.grpc._

import scala.collection.mutable.ListBuffer

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object Search {}

case class SearchCompoundRequest(var id: String, var operation: String, var options: Map[String, String], var targets: Option[Seq[SearchCompoundRequest]]) {
  /**
    *
    */
  def toRPCMessage(): QueryMessage = {
    this.prepare()
    this.cqm()
  }

  private def entity = options.get("entityname").get

  private def subtype = options.get("subtype").getOrElse("")

  private def sparsify(vec: Seq[Float]) = {

    val ii = new ListBuffer[Int]()
    val vv = new ListBuffer[Float]()

    vec.zipWithIndex.foreach { x =>
      val v = x._1
      val i = x._2

      if (math.abs(v) > 1E-10) {
        ii.append(i)
        vv.append(v)
      }
    }

    (vv.toArray, ii.toArray, vec.size)
  }

  private def query = {
    val vals = options.get("query").get.split(",").map(_.toFloat)

    if (options.getOrElse("sparsequery", "") == "true") {
      val (vv, ii, size) = sparsify(vals)
      FeatureVectorMessage().withSparseVector(SparseVectorMessage(vv, ii, size))
    } else {
      FeatureVectorMessage().withDenseVector(DenseVectorMessage(vals))
    }
  }

  private def weights = {
    if (options.get("weights").isDefined && options.get("weights").get.length > 0) {
      val vals = options.get("weights").get.split(",").map(_.toFloat)

      if (options.getOrElse("sparseweights", "") == "true") {
        val (vv, ii, size) = sparsify(vals)
        Some(FeatureVectorMessage().withSparseVector(SparseVectorMessage(vv, ii, size)))
      } else {
        Some(FeatureVectorMessage().withDenseVector(DenseVectorMessage(vals)))
      }
    } else {
      None
    }
  }

  private def distance: DistanceMessage = {
    val distance = options.getOrElse("distance", "")

    distance match {
      case "chisquared" => DistanceMessage(DistanceMessage.DistanceType.chisquared)
      case "correlation" => DistanceMessage(DistanceMessage.DistanceType.correlation)
      case "cosine" => DistanceMessage(DistanceMessage.DistanceType.cosine)
      case "hamming" => DistanceMessage(DistanceMessage.DistanceType.hamming)
      case "jaccard" => DistanceMessage(DistanceMessage.DistanceType.jaccard)
      case "kullbackleibler" => DistanceMessage(DistanceMessage.DistanceType.kullbackleibler)
      case "chebyshev" => DistanceMessage(DistanceMessage.DistanceType.chebyshev)
      case "euclidean" => DistanceMessage(DistanceMessage.DistanceType.euclidean)
      case "squaredeuclidean" => DistanceMessage(DistanceMessage.DistanceType.squaredeuclidean)
      case "manhattan" => DistanceMessage(DistanceMessage.DistanceType.manhattan)
      case "minkowski" => DistanceMessage(DistanceMessage.DistanceType.minkowski)
      case "spannorm" => DistanceMessage(DistanceMessage.DistanceType.spannorm)
      case _ => DistanceMessage(DistanceMessage.DistanceType.minkowski, Map("norm" -> "1"))
    }
  }

  /**
    *
    * @return
    */
  private def nnq = {
    val partitions = if (options.get("partitions").isDefined) {
      val s = options.get("partitions").get
      if (s.contains(",")) {
        s.split(",").map(_.toInt).toSeq
      } else {
        Seq[Int](s.toInt)
      }
    } else {
      Seq[Int]()
    }

    val nnq = NearestNeighbourQueryMessage(options.getOrElse("column", "feature"), Some(query),
      weights, Some(distance),
      options.get("k").getOrElse("100").toInt,
      options, //not overly clean solution, but not problematic to send too much information in this case
      true,
      partitions)

    nnq
  }

  /**
    *
    */
  private def prepare(): Unit = {
    if ((operation == "index" || operation == "sequential" || operation == "external") && targets.isDefined && !targets.get.isEmpty) {
      val from = SearchCompoundRequest(id, operation, options, None)
      val to = targets.get.head

      id = id + "-intersectfilter"
      operation = "aggregation"
      options = Map("subtype" -> "intersect", "operationorder" -> "right")
      targets = Option(Seq(from, to))
    } else if (targets.isDefined) {
      targets.get.foreach { t =>
        t.prepare()
      }
    }
  }


  /**
    *
    * @return
    */
  private def cqm(): QueryMessage = {
    val query = if (targets.isEmpty || targets.get.isEmpty) {
      SubExpressionQueryMessage().withQm(QueryMessage(queryid = "sequential", from = Some(FromMessage().withEntity(entity)), nnq = Option(nnq), hints = Seq("sequential")))
    } else {
      targets.get.head.seqm()
    }

    QueryMessage(
      queryid = id,
      //projection = Some(ProjectionMessage().withField(ProjectionMessage.FieldnameMessage.apply(Seq("id", "adamprodistance")))),
      from = Some(FromMessage().withExpression(query)),
      information = informationLevel())
  }

  /**
    *
    * @return
    */
  private def informationLevel(): Seq[InformationLevel] = {
    val option = options.getOrElse("informationlevel", "full_tree")

    option match {
      case "full_tree" => Seq(INFORMATION_FULL_TREE, INFORMATION_INTERMEDIATE_RESULTS)
      case "final_only" => Seq(INFORMATION_LAST_STEP_ONLY)
      case _ => Seq(INFORMATION_FULL_TREE, INFORMATION_INTERMEDIATE_RESULTS)
    }
  }

  /**
    *
    * @return
    */
  private def eqm(): ExpressionQueryMessage = {
    assert(operation == "aggregation")

    val op = options.get("subtype").get match {
      case "union" => ExpressionQueryMessage.Operation.UNION
      case "intersect" => ExpressionQueryMessage.Operation.INTERSECT
      case "except" => ExpressionQueryMessage.Operation.EXCEPT
    }

    val lsqm = targets.get(0).seqm()
    val rsqm = targets.get(1).seqm()

    val order = options.get("operationorder").get match {
      case "parallel" => ExpressionQueryMessage.OperationOrder.PARALLEL
      case "left" => ExpressionQueryMessage.OperationOrder.LEFTFIRST
      case "right" => ExpressionQueryMessage.OperationOrder.RIGHTFIRST
      case _ => ExpressionQueryMessage.OperationOrder.PARALLEL
    }

    ExpressionQueryMessage(id, Option(lsqm), op, order, Option(rsqm))
  }

  /**
    *
    * @return
    */
  private def seqm(): SubExpressionQueryMessage = {
    var sqm = SubExpressionQueryMessage().withQueryid(id)

    operation match {
      case "aggregation" =>
        sqm = sqm.withEqm(eqm())

      case "index" =>
        if (options.get("indexname").isDefined) {
          sqm = sqm.withQm(ssiqm())
        } else {
          sqm = sqm.withQm(siqm())
        }
      case "sequential" =>
        sqm = sqm.withQm(ssqm())

      case "boolean" =>
        sqm = sqm.withQm(sbqm())

      case "external" =>
        sqm = sqm.withEhqm(ehqm())
    }

    sqm
  }

  private def ssiqm(): QueryMessage = QueryMessage(queryid = id, from = Some(FromMessage().withIndex(options.get("indexname").get)), nnq = Option(nnq))

  private def ssqm(): QueryMessage = QueryMessage(queryid = id, from = Some(FromMessage().withEntity(entity)), nnq = Option(nnq), hints = Seq("sequential"))

  private def siqm(): QueryMessage = QueryMessage(queryid = id, from = Some(FromMessage().withEntity(entity)), hints = Seq(subtype), nnq = Option(nnq))

  private def ehqm(): ExternalHandlerQueryMessage = ExternalHandlerQueryMessage(id, entity, subtype, options)

  private def sbqm(): QueryMessage = QueryMessage(queryid = id, from = Some(FromMessage().withEntity(entity)), bq = Some(BooleanQueryMessage(Seq(WhereMessage(options.get("field").get, options.get("value").get)))))
}

case class SearchCompoundResponse(code: Int, details: SearchResponse)

case class SearchResponse(intermediateResponses: Seq[SearchResponseInfo]) {
  def this(msg: QueryResultsMessage) {
    this(msg.responses.map(ir => new SearchResponseInfo(ir)))
  }
}

case class SearchResponseInfo(id: String, time: Long, results: Seq[Map[String, String]]) {
  def this(msg: QueryResultInfoMessage) {
    this(msg.queryid, msg.time, msg.results.map(result => result.data.map(attribute => {
      val key = attribute._1
      val value = attribute._2.datatype match {
        case Datatype.IntData(x) => x.toInt.toString
        case Datatype.LongData(x) => x.toLong.toString
        case Datatype.FloatData(x) => x.toFloat.toString
        case Datatype.DoubleData(x) => x.toDouble.toString
        case Datatype.StringData(x) => x.toString
        case Datatype.BooleanData(x) => x.toString
        case Datatype.FeatureData(x) => x.feature.denseVector.get.vector.mkString("[", ",", "]")
        case _ => ""
      }
      key -> value
    })))
  }
}


case class SearchProgressiveRequest(val id: String, val entityname: String, attribute: String, query: String, hints: Seq[String], val k: Int) {
  lazy val q = query.split(",").map(_.toFloat)
}

case class SearchProgressiveResponse(results: SearchProgressiveIntermediaryResponse, status: String)

case class SearchProgressiveStartResponse(id: String)

case class SearchProgressiveIntermediaryResponse(id: String, confidence: Double, source: String, sourcetype: String, time: Long, results: Seq[Map[String, String]], status: ProgressiveQueryStatus.Value)

