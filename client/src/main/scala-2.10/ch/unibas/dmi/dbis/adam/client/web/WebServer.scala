package ch.unibas.dmi.dbis.adam.client.web

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.{Controller, HttpServer}


/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class WebServer(port: Int, controller: Controller) extends HttpServer {
  override val defaultFinatraHttpPort: String = ":" + port

  override def configureHttp(router: HttpRouter) {
    router
      .filter[LoggingMDCFilter[Request, Response]]
      .filter[TraceIdMDCFilter[Request, Response]]
      .filter[CommonFilters]
      .add(controller)
  }
}