package org.vitrivr.adampro.rpc.datastructures

import org.vitrivr.adampro.grpc.grpc.{FromMessage, QueryMessage}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
case class RPCHintBasedQueryObject(id : String, options : Map[String, String]) extends RPCGenericQueryObject(id, options){
  override protected def setQueryMessage(qm: QueryMessage): QueryMessage = qm.withFrom(FromMessage().withEntity(entityname)).withHints(hints).withNnq(nnq.get)

  protected def entityname = options.get("entityname").get
  protected def hints = options.get("hints").get.split(",")
}
