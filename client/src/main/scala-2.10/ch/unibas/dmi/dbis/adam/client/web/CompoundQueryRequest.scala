package ch.unibas.dmi.dbis.adam.client.web

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

  private def nnq = NearestNeighbourQueryMessage(query, 2, 100, false, Map())

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
      return CompoundQueryMessage(entity, Option(nnq), None, true);
    }

    val node = targets.get.head

    var sqm = SubExpressionQueryMessage().withId(node.id)

    if (node.operation == "aggregate") {
      sqm = sqm.withEqm(node.eqm())
    } else if (node.options.get("indexname").isDefined) {
      sqm = sqm.withSsiqm(node.ssiqm())
    } else {
      sqm = sqm.withSiqm(node.siqm())
    }

    CompoundQueryMessage(entity, Option(nnq), Option(sqm), true, id)
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

    ExpressionQueryMessage(Option(lsqm), op, order, Option(rsqm), id)
  }

  private def seqm(cqr: CompoundQueryRequest): SubExpressionQueryMessage = {
    var sqm = SubExpressionQueryMessage().withId(cqr.id)

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
    SimpleSpecifiedIndexQueryMessage(indexname, Option(nnq), None, false)
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
    SimpleIndexQueryMessage(entity, indextype, Option(nnq), None, false)
  }
}

