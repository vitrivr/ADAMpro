package org.vitrivr.adampro.rpc.datastructures

import org.vitrivr.adampro.grpc.grpc.{FromMessage, IndexListMessage, QueryMessage}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
case class RPCStochasticScanQueryObject(id : String, options : Map[String, String]) extends RPCGenericQueryObject(id, options){
  override protected def setQueryMessage(qm: QueryMessage): QueryMessage = qm.withFrom(FromMessage().withIndexes(IndexListMessage(indexnames))).withNnq(nnq.get)


  protected def indexnames = options.get("indexnames").get.split(",")
}
