package ch.unibas.dmi.dbis.adam.http.grpc

import ch.unibas.dmi.dbis.adam.http.grpc.adam._

import scala.concurrent.Future

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class AdamSearchImpl extends AdamSearchGrpc.AdamSearch {
  override def doStandardQuery(request: SimpleQueryMessage): Future[QueryResponseListMessage] = ???
  override def doTimedProgressiveQuery(request: TimedQueryMessage): Future[QueryResponseListMessage] = ???
  override def doSequentialQuery(request: SimpleSequentialQueryMessage): Future[QueryResponseListMessage] = ???
  override def doIndexQuery(request: SimpleIndexQueryMessage): Future[QueryResponseListMessage] = ???
}
