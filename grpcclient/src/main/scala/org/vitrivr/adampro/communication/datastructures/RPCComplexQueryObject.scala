package org.vitrivr.adampro.communication.datastructures

import org.vitrivr.adampro.grpc.grpc._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
case class RPCComplexQueryObject(override val id : String, override val options : Map[String, String], override val operation : String, var targets: Option[Seq[RPCGenericQueryObject]]) extends RPCGenericQueryObject(id, options){
  protected def entityname = options.get("entityname").get
  protected def hints() = options.get("hints").map(_.split(",").toSeq).getOrElse(Seq()).filterNot(_.length == 0)


  override protected def setQueryMessage(qm: QueryMessage): QueryMessage = {
    var aqm = qm

    val from = if (options.get("entityname").isDefined) {
        FromMessage().withEntity(entityname)
      } else if (targets.isEmpty || targets.get.isEmpty) {
        FromMessage().withExpression(seqm)
      } else {
        FromMessage().withExpression(targets.get.head.asInstanceOf[RPCComplexQueryObject].seqm)
      }

    aqm = aqm.withFrom(from)

    if(nnq.isDefined){
      aqm = qm.withNnq(nnq.get)
    }

    aqm
  }

  /**
    *
    */
  override def prepare(): RPCGenericQueryObject = {
    var sqm = super.prepare()

    if ((operation == "index" || operation == "sequential" || operation == "external") && targets.isDefined && !targets.get.isEmpty) {
      val from = RPCComplexQueryObject(id, options, operation, None)
      val to = targets.get.head

      RPCComplexQueryObject(id + "-intersectfilter", Map("subtype" -> "intersect", "operationorder" -> "right"), "aggregation",  Option(Seq(from, to))).prepare()
    } else if (targets.isDefined) {
      sqm.asInstanceOf[RPCComplexQueryObject].targets = Some(targets.get.map { t =>
        t.prepare()
      })

      sqm
    } else {
      sqm
    }
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
        sqm = sqm.withQm(RPCIndexScanQueryObject(id, options).buildQueryMessage)

      case "sequential" =>
        sqm = sqm.withQm(RPCSequentialScanQueryObject(id, options).buildQueryMessage)

      case "boolean" =>
        sqm = sqm.withQm(RPCBooleanScanQueryObject(id, options).buildQueryMessage)

      case "external" =>
        sqm = sqm.withEhqm(ExternalHandlerQueryMessage(id, entityname, options.getOrElse("subtype", ""), options))

      case "stochastic" =>
        sqm = sqm.withQm(RPCStochasticScanQueryObject(id, options).buildQueryMessage)

      case "empirical" =>
        sqm = sqm.withQm(RPCEmpiricalScanQueryObject(id, options).buildQueryMessage)

      case "hint" =>
        sqm =  sqm.withQm(RPCHintBasedQueryObject(id, options).buildQueryMessage)
    }

    sqm
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
      case "fuzzyunion" => ExpressionQueryMessage.Operation.FUZZYUNION
      case "fuzzyintersect" => ExpressionQueryMessage.Operation.FUZZYINTERSECT
    }

    val lsqm = targets.get(0).asInstanceOf[RPCComplexQueryObject].seqm()
    val rsqm = targets.get(1).asInstanceOf[RPCComplexQueryObject].seqm()

    val order = options.get("operationorder").get match {
      case "parallel" => ExpressionQueryMessage.OperationOrder.PARALLEL
      case "left" => ExpressionQueryMessage.OperationOrder.LEFTFIRST
      case "right" => ExpressionQueryMessage.OperationOrder.RIGHTFIRST
      case _ => ExpressionQueryMessage.OperationOrder.PARALLEL
    }

    ExpressionQueryMessage(id, Option(lsqm), op, order, Option(rsqm))
  }


}
