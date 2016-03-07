package ch.unibas.dmi.dbis.adam.http.grpc

import ch.unibas.dmi.dbis.adam.http.grpc.adam.{AdamSearchStreamingGrpc, QueryResponseListMessage, SimpleQueryMessage}

import scala.concurrent.Future

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class AdamSearchStreamingImpl extends AdamSearchStreamingGrpc.AdamSearchStreaming {
  override def doProgressiveQuery(request: SimpleQueryMessage): Future[QueryResponseListMessage] = ???
}
