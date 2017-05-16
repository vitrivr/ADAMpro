package org.vitrivr.adampro.rpc.datastructures
import org.vitrivr.adampro.grpc.grpc.{FromMessage, QueryMessage}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
case class RPCIndexScanQueryObject(id : String, options : Map[String, String]) extends RPCGenericQueryObject(id, options){
  override protected def setQueryMessage(qm: QueryMessage): QueryMessage = {
    if(opindexname.isDefined){
      qm.withFrom(FromMessage().withIndex(opindexname.get)).withNnq(nnq.get)
    } else if(opentityname.isDefined && ophints.isDefined){
      qm.withFrom(FromMessage().withEntity(opentityname.get)).withNnq(nnq.get).withHints(ophints.get)
    } else {
      throw new Exception("either indexname or entityname and hints has to be defined")
    }
  }

  protected def opentityname = options.get("entityname")
  protected def opindexname = options.get("indexname")
  protected def ophints = options.get("hints").map(_.split(","))

}
