package ch.unibas.dmi.dbis.adam.client.web.datastructures

import ch.unibas.dmi.dbis.adam.client.web.ProgressiveQueryStatus
import ch.unibas.dmi.dbis.adam.rpc.datastructures.{RPCQueryObject, RPCQueryResults}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
private[client] object Search {}

private[client] case class SearchCompoundRequest(var id: String, var operation: String, var options: Map[String, String], var targets: Option[Seq[SearchCompoundRequest]]) {
  def toRPCQueryObject : RPCQueryObject = {
    RPCQueryObject(id, operation, options, targets.map(_.map(_.toRPCQueryObject)))
  }
}

private[client] case class SearchCompoundResponse(code: Int, details: SearchResponse)

private[client] case class SearchResponse(intermediateResponses: Seq[RPCQueryResults]) {}

private[client] case class SearchResponseInfo(id: String, time: Long, results: Seq[Map[String, String]]) {
  def this(qr: RPCQueryResults) {
    this(qr.id, qr.time, qr.results)
  }
}


private[client] case class SearchProgressiveRequest(val id: String, val entityname: String, attribute: String, query: String, hints: Seq[String], val k: Int) {
  lazy val q = query.split(",").map(_.toFloat)
}

private[client] case class SearchProgressiveResponse(results: SearchProgressiveIntermediaryResponse, status: String)

private[client] case class SearchProgressiveStartResponse(id: String)

private[client] case class SearchProgressiveIntermediaryResponse(id: String, confidence: Double, source: String, sourcetype: String, time: Long, results: Seq[Map[String, String]], status: ProgressiveQueryStatus.Value)

