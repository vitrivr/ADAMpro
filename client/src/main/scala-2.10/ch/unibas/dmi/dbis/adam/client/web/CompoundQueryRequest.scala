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
    val cqm = CompoundQueryMessage(entity, Option(nnq), true)

    val node = targets.get.head
    if (node.operation == "aggregate") {
      cqm.withEqm(node.eqm())
    } else if (node.options.get("indexname").isDefined) {
      cqm.withSsiqm(node.ssiqm())
    } else {
      cqm.withSiqm(node.siqm())
    }

    cqm
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

    val eq = ExpressionQueryMessage(operation = op)

    val left = targets.get(0)
    if (left.operation == "aggregate") {
      eq.withLeqm(left.eqm())
    } else if (left.options.get("indexname").isDefined) {
      eq.withLssiqm(left.ssiqm())
    } else {
      eq.withLsiqm(left.siqm())
    }

    val right = targets.get(1)
    if (right.operation == "aggregate") {
      eq.withReqm(right.eqm())
    } else if (right.options.get("indexname").isDefined) {
      eq.withRssiqm(right.ssiqm())
    } else {
      eq.withRsiqm(right.siqm())
    }

    eq
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

