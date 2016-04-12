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

  case class PreparationRequestReponse(code: Int, entityname: String = "", ntuples: Int = 0, ndims: Int = 0)

  post("/add-entity") { request: PreparationRequest =>
    val res = rpcClient.prepareDemo(request.entityname, request.ntuples, request.ndims)

    log.info("prepared data")

    if (res) {
      response.ok.json(PreparationRequestReponse(200, request.entityname, request.ntuples, request.ndims))
    } else {
      response.ok.json(PreparationRequestReponse(500))
    }
  }


  post("/query") { request: CompoundQueryRequest =>
    log.info("query: " + request)
    val res = rpcClient.compoundQuery(request)
    response.ok.json(res)
  }
}
