package org.vitrivr.adampro.rpc.datastructures

import org.vitrivr.adampro.grpc.grpc.{FromMessage, QueryMessage}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
case class RPCEmpiricalScanQueryObject(id : String, options : Map[String, String]) extends RPCGenericQueryObject(id, options){
  override protected def setQueryMessage(qm: QueryMessage): QueryMessage = qm.withFrom(FromMessage().withEntity(entityname)).withNnq(nnq.get).withHints(subtype)

  protected def entityname = options.get("entityname").get
  protected def subtype = Seq(options.get("subtype").get)
}
