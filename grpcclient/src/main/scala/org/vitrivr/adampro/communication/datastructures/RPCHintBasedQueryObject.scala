package org.vitrivr.adampro.communication.datastructures

import org.vitrivr.adampro.grpc.grpc.{FromMessage, QueryMessage}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
case class RPCHintBasedQueryObject(override val id : String, override val options : Map[String, String]) extends RPCGenericQueryObject(id, options){
  override val operation = "hint"

  override protected def setQueryMessage(qm: QueryMessage): QueryMessage = qm.withFrom(FromMessage().withEntity(entityname)).withHints(hints).withNnq(nnq.get).withNoFallback(nofallback)

  protected def entityname = options.get("entityname").get
  protected def hints = options.get("hints").get.split(",")
  protected def nofallback = options.get("nofallback").map(_.toBoolean).getOrElse(false)

}
