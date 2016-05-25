package ch.unibas.dmi.dbis.adam.client.web.datastructures

import ch.unibas.dmi.dbis.adam.http.grpc.{QueryResultsMessage, QueryResultInfoMessage}

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
case class CompoundQueryDetails(intermediateResponses : Seq[QueryResponseInfo]) {
  def this(msg : QueryResultsMessage){
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
  def this(msg : QueryResultInfoMessage){
    this(msg.queryid, msg.time, msg.results.length)
  }
}
