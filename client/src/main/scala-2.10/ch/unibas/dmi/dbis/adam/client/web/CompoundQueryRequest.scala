package ch.unibas.dmi.dbis.adam.client.web

import ch.unibas.dmi.dbis.adam.http.grpc._

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
case class CompoundQueryRequest(operation: String, options: Map[String, String],
                                targets: Option[Seq[CompoundQueryRequest]]) //for UNION, INTERSECT and EXCEPT this field contains the sub-queries
{
  //TODO: clean and refactor code!

  /**
    *
    */
  def toRPCMessage(): CompoundQueryMessage = {
    this.cqm()
  }

  private def entity = options.get("entityname").get

  private def query = options.get("query").get.split(",").map(_.toFloat)

  private def nnq = NearestNeighbourQueryMessage(query, 2, 100, false, Map())


  /**
    *
    * @return
    */
  private def cqm(): CompoundQueryMessage = {
    val sqm = SubExpressionQueryMessage()

    val node = targets.get.head
    if (node.operation == "aggregate") {
      sqm.withEqm(node.eqm())
    } else if (node.options.get("indexname").isDefined) {
      sqm.withSsiqm(node.ssiqm())
    } else {
      sqm.withSiqm(node.siqm())
    }

    CompoundQueryMessage(entity, Option(nnq), Option(sqm), true)
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

    ExpressionQueryMessage(Option(lsqm), op, ExpressionQueryMessage.OperationOrder.PARALLEL, Option(rsqm))
  }

  private def seqm(cqr : CompoundQueryRequest) : SubExpressionQueryMessage = {
    val sqm = SubExpressionQueryMessage()
    if (cqr.operation == "aggregate") {
      sqm.withEqm(cqr.eqm())
    } else if (cqr.options.get("indexname").isDefined) {
      sqm.withSsiqm(cqr.ssiqm())
    } else {
      sqm.withSiqm(cqr.siqm())
    }
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

