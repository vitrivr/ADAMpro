package ch.unibas.dmi.dbis.adam.client.web

import ch.unibas.dmi.dbis.adam.client.grpc.RPCClient
import ch.unibas.dmi.dbis.adam.client.web.datastructures._
import ch.unibas.dmi.dbis.adam.http.grpc.IndexType
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Try, Success}

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


  /**
    *
    */
  get("/entity/list") { request: Request =>
    val filter = request.params.get("filter")

    val entities = if (filter.isEmpty) {
      rpcClient.entityList()
    } else {
      Success(Seq(filter.get))
    }

    if (entities.isSuccess) {
      response.ok.json(EntityListResponse(200, entities.get))
    } else {
      response.ok.json(GeneralResponse(500, entities.failed.get.getMessage))
    }
  }


  /**
    *
    */
  get("/entity/details") { request: Request =>
    val entityname = request.params.get("entityname")

    if (entityname.isEmpty) {
      response.ok.json(GeneralResponse(500, "entity not specified"))
    }

    val details = rpcClient.entityDetails(entityname.get)

    if (details.isSuccess) {
      response.ok.json(EntityDetailResponse(200, entityname.get, details.get))
    } else {
      response.ok.json(GeneralResponse(500, details.failed.get.getMessage))
    }
  }


  /**
    *
    */
  get("/entity/benchmark") { request: Request =>
    val entityname = request.params.get("entityname")
    val attribute = request.params.get("attribute")

    if (entityname.isEmpty) {
      response.ok.json(GeneralResponse(500, "entity not specified"))
    }

    if (attribute.isEmpty) {
      response.ok.json(GeneralResponse(500, "attribute not specified"))
    }

    val details = rpcClient.entityBenchmark(entityname.get, attribute.get)

    if (details.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, details.failed.get.getMessage))
    }
  }

  /**
    *
    */
  get("/entity/sparsify") { request: Request =>
    val entityname = request.params.get("entityname")
    val attribute = request.params.get("attribute")

    if (entityname.isEmpty) {
      response.ok.json(GeneralResponse(500, "entity not specified"))
    }

    if (attribute.isEmpty) {
      response.ok.json(GeneralResponse(500, "attribute not specified"))
    }

    val details = rpcClient.entitySparsify(entityname.get, attribute.get)

    if (details.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, details.failed.get.getMessage))
    }
  }

  /**
    *
    */
  get("/entity/drop") { request: Request =>
    val entityname = request.params.get("entityname")

    if (entityname.isEmpty) {
      response.ok.json(GeneralResponse(500, "entity not specified"))
    }

    val details = rpcClient.entityDrop(entityname.get)

    if (details.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, details.failed.get.getMessage))
    }
  }


  /**
    *
    */
  get("/entity/preview") { request: Request =>
    val entityname = request.params.get("entityname")

    if (entityname.isEmpty) {
      response.ok.json(GeneralResponse(500, "entity not specified"))
    }

    val res = rpcClient.entityRead(entityname.get)

    if (res.isSuccess) {
      response.ok.json(EntityReadResponse(200, entityname.get, res.get))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }


  /**
    *
    */
  post("/entity/add") { request: EntityCreateRequest =>
    val res = rpcClient.entityCreate(request.entityname, request.attributes)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200, res.get))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }

  /**
    *
    */
  post("/import") { request: EntityImportRequest =>
    val res = rpcClient.entityImport(request.host, request.database, request.username, request.password)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }

  /**
    *
    */
  post("/entity/insertdemo") { request: EntityFillRequest =>
    val res = rpcClient.entityGenerateRandomData(request.entityname, request.ntuples, request.ndims, 0, 0, 1, false)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }

  /**
    *
    */
  post("/entity/indexall") { request: IndexCreateAllRequest =>
    val res = rpcClient.entityCreateAll(request.entityname, request.attributes, 2)


    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }


  /**
    *
    */
  post("/entity/partition") { request: EntityPartitionRequest =>
    val res = rpcClient.entityPartition(request.entityname, request.npartitions, request.attributes.filter(_.length > 0), request.materialize, request.replace)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200, res.get))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }

  /**
    *
    */
  post("/entity/index/add") { request: IndexCreateRequest =>
    val indextype = request.indextype match {
      case "ecp" => IndexType.ecp
      case "lsh" => IndexType.lsh
      case "mi" => IndexType.mi
      case "pq" => IndexType.pq
      case "sh" => IndexType.sh
      case "vaf" => IndexType.vaf
      case "vav" => IndexType.vav
      case _ => null
    }

    val res = rpcClient.indexCreate(request.entityname, request.attribute, indextype, request.norm, request.options)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }


  /**
    *
    */
  post("/entity/index/partition") { request: IndexPartitionRequest =>
    val res = rpcClient.indexPartition(request.indexname, request.npartitions, request.attributes.filter(_.length > 0), request.materialize, request.replace)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200, res.get))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }

  /**
    *
    */
  post("/search/compound") { request: SearchCompoundRequest =>
    val res = rpcClient.searchCompound(request)
    if (res.isSuccess) {
      response.ok.json(SearchCompoundResponse(200, res.get))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }


  /**
    *
    */
  post("/search/progressive") { request: SearchProgressiveRequest =>
    val res = rpcClient.searchProgressive(request.id, request.entityname, request.q, request.attribute, request.hints, request.k, processProgressiveResults(request.id), completedProgressiveResults)

    progTempResults.synchronized {
      if (!progTempResults.contains(request.id)) {
        val queue = mutable.Queue[SearchProgressiveIntermediaryResponse]()
        progTempResults.put(request.id, queue)
      } else {
        log.error("query id is already being used")
      }
    }

    response.ok.json(SearchProgressiveStartResponse(request.id))
  }


  private def processProgressiveResults(id : String)(res: Try[(String, Double, String, Long, Seq[Map[String, String]])]): Unit = {
    if (res.isSuccess) {

      val confidence = res.get._2
      val source = res.get._3
      val time = res.get._4
      val results = res.get._5


      val sourcetype = if (source.length > 0) {
        source.substring(0, source.indexOf("(")).toLowerCase.trim
      } else {
        ""
      }
      progTempResults.get(id).get += SearchProgressiveIntermediaryResponse(id, confidence, source, sourcetype, time, results, ProgressiveQueryStatus.RUNNING)
    } else {
      log.error(res.failed.get)
      completedProgressiveResults(id, res.failed.get.getMessage, ProgressiveQueryStatus.ERROR)
    }
  }


  val progTempResults = mutable.HashMap[String, mutable.Queue[SearchProgressiveIntermediaryResponse]]()

  private def completedProgressiveResults(id: String): Unit = {
    completedProgressiveResults(id, "", ProgressiveQueryStatus.FINISHED)
  }

  private def completedProgressiveResults(id : String, message : String, newStatus : ProgressiveQueryStatus.Value): Unit ={
    progTempResults.get(id).get += SearchProgressiveIntermediaryResponse(id, 0.0, message, "", 0, Seq(), newStatus)
    lazy val f = Future {
      Thread.sleep(10000);
      true
    }
    Await.result(f, 10 second)
    progTempResults.remove(id)
  }


  /**
    *
    */
  get("/query/progressive/temp") { request: Request =>
    val id = request.params.get("id").get

    progTempResults.synchronized {
      if (progTempResults.get(id).isDefined && !progTempResults.get(id).get.isEmpty) {
        val result = progTempResults.get(id).get.dequeue()
        response.ok.json(SearchProgressiveResponse(result, result.status.toString))
      } else {
        response.ok
      }
    }
  }


}

/**
  *
  */
object ProgressiveQueryStatus extends Enumeration {
  val RUNNING = Value("running")
  val PREMATURE_FINISHED = Value("premature")
  val FINISHED = Value("finished")
  val ERROR = Value("error")
}
