package ch.unibas.dmi.dbis.adam.client.web

import ch.unibas.dmi.dbis.adam.client.grpc.RPCClient
import ch.unibas.dmi.dbis.adam.http.grpc.IndexType
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

  /**
    *
    */
  post("/entity/add") { request: PreparationRequest =>
    val res = rpcClient.prepareDemo(request.entityname, request.ntuples, request.ndims)

    log.info("prepared data")

    if (res) {
      response.ok.json(PreparationRequestReponse(200, request.entityname, request.ntuples, request.ndims))
    } else {
      response.ok.json(PreparationRequestReponse(500))
    }
  }

  case class IndexRequestResponse(code: Int, entityname: String = "")

  /**
    *
    */
  post("/index/add") { request: IndexRequest =>
    val indextype = request.indextype match {
      case "ecp" => IndexType.ecp
      case "lsh" => IndexType.lsh
      case "pq" => IndexType.pq
      case "sh" => IndexType.sh
      case "vaf" => IndexType.vaf
      case "vav" => IndexType.vav
      case _ => null
    }

    val res = rpcClient.addIndex(request.entityname, indextype, request.norm, request.options)

    log.info("created index")

    if (!res.isEmpty) {
      response.ok.json(IndexRequestResponse(200, res))
    } else {
      response.ok.json(IndexRequestResponse(500))
    }
  }

  /**
    *
    */
  post("/query") { request: CompoundQueryRequest =>
    log.info("query: " + request)
    val res = rpcClient.compoundQuery(request)
    response.ok.json(res)
  }
}
