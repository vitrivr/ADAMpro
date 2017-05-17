package org.vitrivr.adampro.rpc.datastructures

import org.vitrivr.adampro.grpc.grpc.BooleanQueryMessage.WhereMessage
import org.vitrivr.adampro.grpc.grpc.{BooleanQueryMessage, DataMessage, FromMessage, QueryMessage}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
case class RPCBooleanScanQueryObject(override val id : String, override val options : Map[String, String]) extends RPCGenericQueryObject(id, options){
  override val operation = "boolean"

  override protected def setQueryMessage(qm: QueryMessage): QueryMessage = qm.withFrom(FromMessage().withEntity(entityname)).withBq(BooleanQueryMessage(Seq(WhereMessage(attribute, Seq(DataMessage().withStringData(value))))))

  protected def entityname = options.get("entityname").get
  protected def attribute = options.get("attribute").get
  protected def value = options.get("value").get
}
