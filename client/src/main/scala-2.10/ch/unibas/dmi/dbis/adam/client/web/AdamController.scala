package ch.unibas.dmi.dbis.adam.client.web

import ch.unibas.dmi.dbis.adam.client.grpc.RPCClient
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller
import org.apache.log4j.Logger

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class AdamController(rpcClient: RPCClient) extends Controller {
  val log = Logger.getLogger(getClass.getName)

  get("/:*") { request: Request =>
    response.ok.fileOrIndex(
      request.params("*"),
      "index.html")
  }

  post("/prepare") { request: PreparationRequest =>
    log.info("prepare: " + request)
    rpcClient.prepareDemo(request.entityname, 5000, 100)
    response.ok.html("ok")
  }

  post("/query") { request: CompoundQueryRequest =>
    log.info("query: " + request)
    rpcClient.compoundQuery(request)
    response.ok.html("ok")
  }
}
