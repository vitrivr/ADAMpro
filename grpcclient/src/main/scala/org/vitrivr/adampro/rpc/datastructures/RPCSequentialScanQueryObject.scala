package org.vitrivr.adampro.rpc.datastructures
import org.vitrivr.adampro.grpc.grpc.{FromMessage, QueryMessage}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
case class RPCSequentialScanQueryObject(override val id: String, override val options : Map[String, String]) extends RPCGenericQueryObject(id, options){
  override val operation = "sequential"

  override protected def setQueryMessage(qm: QueryMessage): QueryMessage = qm.withFrom(FromMessage().withEntity(entityname)).withHints(Seq("sequential")).withNnq(nnq.get)

  protected def entityname = options.get("entityname").get
}
