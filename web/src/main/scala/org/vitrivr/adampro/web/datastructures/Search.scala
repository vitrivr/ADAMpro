package org.vitrivr.adampro.web.datastructures

import org.vitrivr.adampro.communication.datastructures.{RPCComplexQueryObject, RPCQueryResults}
import org.vitrivr.adampro.web.controller.ProgressiveQueryStatus

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
private[web] object Search {}

private[web] case class SearchRequest(var id: String, var operation: String, var options: Map[String, String], var targets: Option[Seq[SearchRequest]]) {
  def toRPCQueryObject : RPCComplexQueryObject = {
    RPCComplexQueryObject(id, options, operation, targets.map(_.map(_.toRPCQueryObject)))
  }
}

private[web] case class SearchRequestJson(json : String)

private[web] case class SearchCompoundResponse(code: Int, details: SearchResponse)

private[web] case class SearchResponse(intermediateResponses: Seq[RPCQueryResults]) {}

private[web] case class SearchResponseInfo(id: String, time: Long, results: Seq[Map[String, String]]) {
  def this(qr: RPCQueryResults) {
    this(qr.id, qr.time, qr.results)
  }
}

private[web] case class SearchParallelResponse(results: SearchParallelIntermediaryResponse, status: String)

private[web] case class SearchParallelStartResponse(id: String)

private[web] case class SearchParallelIntermediaryResponse(id: String, confidence: Double, source: String, time: Long, results: Seq[Map[String, String]], status: ProgressiveQueryStatus.Value)

