package org.vitrivr.adampro.rpc.datastructures

import org.vitrivr.adampro.grpc.grpc.{FromMessage, QueryMessage}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
case class RPCEmpiricalScanQueryObject(override val id : String, override val options : Map[String, String]) extends RPCGenericQueryObject(id, options){
  override val operation = "empirical"

  override protected def setQueryMessage(qm: QueryMessage): QueryMessage = qm.withFrom(FromMessage().withEntity(entityname)).withNnq(nnq.get).withHints(hints)

  protected def entityname = options.get("entityname").get
  protected def hints = Seq(options.get("hints").get)
}
