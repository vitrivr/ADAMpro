package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.http.grpc.adam.{QueryResponseListMessage, SimpleQueryMessage, AdamSearchStreamingGrpc}

import scala.concurrent.Future

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class SearchStreamingImpl extends AdamSearchStreamingGrpc.AdamSearchStreaming {
  override def doProgressiveQuery(request: SimpleQueryMessage): Future[QueryResponseListMessage] = ???
}
