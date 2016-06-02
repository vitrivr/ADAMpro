package ch.unibas.dmi.dbis.adam.client.web.datastructures

import ch.unibas.dmi.dbis.adam.http.grpc.BooleanQueryMessage.WhereMessage
import ch.unibas.dmi.dbis.adam.http.grpc.QueryMessage.InformationLevel.{INFORMATION_INTERMEDIATE_RESULTS, INFORMATION_FULL_TREE, INFORMATION_LAST_STEP_ONLY}
import ch.unibas.dmi.dbis.adam.http.grpc.QueryMessage.{InformationLevel}
import ch.unibas.dmi.dbis.adam.http.grpc._

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
case class CompoundQueryRequest(var id: String, var operation: String, var options: Map[String, String],
                                var targets: Option[Seq[CompoundQueryRequest]]) //for UNION, INTERSECT and EXCEPT this field contains the sub-queries
{
  /**
    *
    */
  def toRPCMessage(): QueryMessage = {
    this.prepare()
    this.cqm()
  }

  /**
    * 
    * @return
    */
  private def entity = options.get("entityname").get

  /**
    *
    * @return
    */
  private def subtype = options.get("subtype").getOrElse("")

  /**
    *
    * @return
    */
  private def query = options.get("query").get.split(",").map(_.toFloat)

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

    val nnq = NearestNeighbourQueryMessage(options.getOrElse("column", "feature"), Some(FeatureVectorMessage().withDenseVector(DenseVectorMessage(query))),
      None,
      Some(DistanceMessage(DistanceMessage.DistanceType.minkowski, Map("norm" -> "1"))),
      options.get("k").getOrElse("100").toInt,
      Map(),
      true,
      partitions)

    nnq
  }

  /**
    *
    */
  private def prepare(): Unit = {
    if ((operation == "index" || operation == "sequential" || operation == "external") && targets.isDefined && !targets.get.isEmpty) {
      val from = CompoundQueryRequest(id, operation, options, None)
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
      projection = Some(ProjectionMessage().withField(ProjectionMessage.FieldnameMessage.apply(Seq("id", "adamprodistance")))),
      from = Some(FromMessage().withExpression(query)),
      information = informationLevel())
  }

  /**
    *
    * @return
    */
  private def informationLevel() : Seq[InformationLevel] = {
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

  /**
    *
    * @return
    */
  private def ssiqm(): QueryMessage = {
    assert(operation == "index")
    val indexname = options.get("indexname").get
    QueryMessage(queryid = id, from = Some(FromMessage().withIndex(indexname)), nnq = Option(nnq))
  }

  /**
    *
    * @return
    */
  private def ssqm(): QueryMessage = {
    assert(operation == "sequential")
    QueryMessage(queryid = id, from = Some(FromMessage().withEntity(entity)), nnq = Option(nnq), hints = Seq("sequential"))
  }


  /**
    *
    * @return
    */
  private def siqm(): QueryMessage = {
    assert(operation == "index")
    QueryMessage(queryid = id, from = Some(FromMessage().withEntity(entity)), hints = Seq(subtype), nnq = Option(nnq))
  }

  /**
    *
    * @return
    */
  private def ehqm(): ExternalHandlerQueryMessage = {
    ExternalHandlerQueryMessage(id, entity, subtype, options)
  }

  /**
    *
    * @return
    */
  private def sbqm() : QueryMessage = {
    QueryMessage(queryid = id, from = Some(FromMessage().withEntity(entity)), bq = Some(BooleanQueryMessage(Seq(WhereMessage(options.get("field").get, options.get("value").get)))))
  }

}

