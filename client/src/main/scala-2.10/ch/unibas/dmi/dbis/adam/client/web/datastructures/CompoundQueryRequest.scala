package ch.unibas.dmi.dbis.adam.client.web.datastructures

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

  private def query = options.get("query").get.split(",").map(_.toFloat)

  private def nnq = NearestNeighbourQueryMessage(query, Some(DistanceMessage(DistanceMessage.DistanceType.minkowski, Map("norm" -> "1"))), options.get("k").getOrElse("100").toInt, true, Map())

  /**
    *
    */
  private def prepare(): Unit = {
    if (operation == "indexscan" && targets.isDefined && targets.get.length > 0) {
      val from = CompoundQueryRequest(id, operation, options, None)
      val to = targets.get.head

      id = id + "-intersectfilter"
      operation = "aggregate"
      options = Map("aggregation" -> "intersect", "operationorder" -> "right")
      targets = Option(Seq(from, to))
    } else if(targets.isDefined) {
      targets.get.foreach{t =>
        t.prepare()
      }
    }
  }


  /**
    *
    * @return
    */
  private def cqm(): CompoundQueryMessage = {
    if (targets.get.isEmpty) {
      val sqm = SubExpressionQueryMessage().withSsqm(SimpleSequentialQueryMessage(id, entity, Option(nnq), None, true))
      return CompoundQueryMessage(id, entity, Option(nnq), None, Option(sqm), true, true);
    }

    val node = targets.get.head

    var sqm = SubExpressionQueryMessage().withQueryid(node.id)

    if (node.operation == "aggregate") {
      sqm = sqm.withEqm(node.eqm())
    } else if (node.options.get("indexname").isDefined) {
      sqm = sqm.withSsiqm(node.ssiqm())
    } else {
      sqm = sqm.withSiqm(node.siqm())
    }

    CompoundQueryMessage(id, entity, Option(nnq), None, Option(sqm), true, true)
  }

  /**
    *
    * @return
    */
  private def eqm(): ExpressionQueryMessage = {
    val op = options.get("aggregation").get match {
      case "union" => ExpressionQueryMessage.Operation.UNION
      case "intersect" => ExpressionQueryMessage.Operation.INTERSECT
      case "except" => ExpressionQueryMessage.Operation.EXCEPT
    }

    val lsqm = seqm(targets.get(0))
    val rsqm = seqm(targets.get(1))

    val order = options.get("operationorder").get match {
      case "parallel" => ExpressionQueryMessage.OperationOrder.PARALLEL
      case "left" => ExpressionQueryMessage.OperationOrder.LEFTFIRST
      case "right" => ExpressionQueryMessage.OperationOrder.RIGHTFIRST
      case _  => ExpressionQueryMessage.OperationOrder.PARALLEL
    }

    ExpressionQueryMessage(id, Option(lsqm), op, order, Option(rsqm))
  }

  private def seqm(cqr: CompoundQueryRequest): SubExpressionQueryMessage = {
    var sqm = SubExpressionQueryMessage().withQueryid(cqr.id)

    if (cqr.operation == "aggregate") {
      sqm = sqm.withEqm(cqr.eqm())
    } else if (cqr.options.get("indexname").isDefined) {
      sqm = sqm.withSsiqm(cqr.ssiqm())
    } else {
      sqm = sqm.withSiqm(cqr.siqm())
    }
    sqm
  }

  /**
    *
    * @return
    */
  private def ssiqm(): SimpleSpecifiedIndexQueryMessage = {
    val indexname = options.get("indexname").get
    SimpleSpecifiedIndexQueryMessage(id, indexname, Option(nnq), None, false)
  }


  /**
    *
    * @return
    */
  private def siqm(): SimpleIndexQueryMessage = {
    val indextype = options.get("indextype").get match {
      case "ecp" => IndexType.ecp
      case "lsh" => IndexType.lsh
      case "pq" => IndexType.pq
      case "sh" => IndexType.sh
      case "vaf" => IndexType.vaf
      case "vav" => IndexType.vav
    }
    SimpleIndexQueryMessage(id, entity, indextype, Option(nnq), None, false)
  }
}

