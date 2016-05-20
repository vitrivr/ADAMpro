package ch.unibas.dmi.dbis.adam.client.web.datastructures

import ch.unibas.dmi.dbis.adam.http.grpc.BooleanQueryMessage.WhereMessage
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
  def toRPCMessage(): CompoundQueryMessage = {
    this.prepare()
    this.cqm()
  }

  private def entity = options.get("entityname").get

  private def subtype = options.get("subtype").getOrElse("")

  private def query = options.get("query").get.split(",").map(_.toFloat)

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
      Some(DistanceMessage(DistanceMessage.DistanceType.minkowski, Map("norm" -> "1"))),
      options.get("k").getOrElse("100").toInt,
      true,
      Map(),
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
  private def cqm(): CompoundQueryMessage = {
    if (targets.isEmpty || targets.get.isEmpty) {
      val sqm = SubExpressionQueryMessage().withSsqm(SimpleSequentialQueryMessage("sequential", entity, Option(nnq), None, true))
      return CompoundQueryMessage(id, entity, Option(nnq), None, Option(sqm), true, true);
    }

    val node = targets.get.head

    CompoundQueryMessage(id, entity, Option(nnq), None, Option(node.seqm()), true, true)
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
          sqm = sqm.withSsiqm(ssiqm())
        } else {
          sqm = sqm.withSiqm(siqm())
        }
      case "sequential" =>
        sqm = sqm.withSsqm(ssqm())

      case "boolean" =>
        sqm = sqm.withSbqm(sbqm())

      case "external" =>
        sqm = sqm.withEhqm(ehqm())
    }

    sqm
  }

  /**
    *
    * @return
    */
  private def ssiqm(): SimpleSpecifiedIndexQueryMessage = {
    assert(operation == "index")

    val indexname = options.get("indexname").get
    SimpleSpecifiedIndexQueryMessage(id, indexname, Option(nnq), None)
  }

  /**
    *
    * @return
    */
  private def ssqm(): SimpleSequentialQueryMessage = {
    assert(operation == "sequential")

    SimpleSequentialQueryMessage(id, entity, Option(nnq), None)
  }


  /**
    *
    * @return
    */
  private def siqm(): SimpleIndexQueryMessage = {
    assert(operation == "index")

    val indextype = subtype match {
      case "ecp" => IndexType.ecp
      case "lsh" => IndexType.lsh
      case "pq" => IndexType.pq
      case "sh" => IndexType.sh
      case "vaf" => IndexType.vaf
      case "vav" => IndexType.vav
    }
    SimpleIndexQueryMessage(id, entity, indextype, Option(nnq), None)
  }

  /**
    *
    * @return
    */
  private def ehqm(): ExternalHandlerQueryMessage = {
    ExternalHandlerQueryMessage(id, entity, subtype, options)
  }

  private def sbqm() : SimpleBooleanQueryMessage = {
    SimpleBooleanQueryMessage(id, entity, Some(BooleanQueryMessage(Seq(WhereMessage(options.get("field").get, options.get("value").get)))))
  }

}

