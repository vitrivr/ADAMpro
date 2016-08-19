package ch.unibas.dmi.dbis.adam.web.datastructures

import ch.unibas.dmi.dbis.adam.rpc.datastructures.{RPCQueryObject, RPCQueryResults}
import ch.unibas.dmi.dbis.adam.web.ProgressiveQueryStatus

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
private[web] object Search {}

private[web] case class SearchRequest(var id: String, var operation: String, var options: Map[String, String], var targets: Option[Seq[SearchRequest]]) {
  def toRPCQueryObject : RPCQueryObject = {
    RPCQueryObject(id, operation, options, targets.map(_.map(_.toRPCQueryObject)))
  }
}

private[web] case class SearchCompoundResponse(code: Int, details: SearchResponse)

private[web] case class SearchResponse(intermediateResponses: Seq[RPCQueryResults]) {}

private[web] case class SearchResponseInfo(id: String, time: Long, results: Seq[Map[String, String]]) {
  def this(qr: RPCQueryResults) {
    this(qr.id, qr.time, qr.results)
  }
}

private[web] case class SearchProgressiveResponse(results: SearchProgressiveIntermediaryResponse, status: String)

private[web] case class SearchProgressiveStartResponse(id: String)

private[web] case class SearchProgressiveIntermediaryResponse(id: String, confidence: Double, source: String, time: Long, results: Seq[Map[String, String]], status: ProgressiveQueryStatus.Value)

