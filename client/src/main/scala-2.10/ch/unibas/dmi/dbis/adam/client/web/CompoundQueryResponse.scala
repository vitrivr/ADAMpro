package ch.unibas.dmi.dbis.adam.client.web

import ch.unibas.dmi.dbis.adam.http.grpc.{CompoundQueryResponseListMessage, QueryResponseInfoMessage}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
/**
  *
  * @param intermediateResponses
  */
case class CompoundQueryResponse(intermediateResponses : Seq[QueryResponseInfo]) {
  def this(msg : CompoundQueryResponseListMessage){
    this(msg.responses.map(ir => new QueryResponseInfo(ir)))
  }
}

/**
  *
  * @param id
  * @param time
  * @param length
  */
case class QueryResponseInfo(id : String, time : Long, length : Int) {
  def this(msg : QueryResponseInfoMessage){
    this(msg.id, msg.time, msg.queryResponseList.get.responses.length)
  }
}
