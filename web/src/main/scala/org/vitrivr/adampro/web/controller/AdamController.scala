package org.vitrivr.adampro.web.controller

import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller
import org.vitrivr.adampro.rpc.RPCClient
import org.vitrivr.adampro.rpc.datastructures.{RPCAttributeDefinition, RPCQueryResults}
import org.vitrivr.adampro.utils.Logging
import org.vitrivr.adampro.web.datastructures._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class AdamController(rpcClient: RPCClient) extends Controller with Logging {
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

    val res = if (filter.isEmpty) {
      rpcClient.entityList()
    } else {
      Success(Seq(filter.get))
    }

    if (res.isSuccess) {
      response.ok.json(EntityListResponse(200, res.get))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }


  /**
    *
    */
  get("/entity/details") { request: Request =>
    val entityname = request.params.get("entityname")
    val attribute = request.params.get("attribute")

    if (entityname.isEmpty) {
      response.ok.json(GeneralResponse(500, "entity not specified"))
    }

    val res = if (attribute.isEmpty) {
      rpcClient.entityDetails(entityname.get).map(EntityDetailResponse(200, entityname.get, "", _))
    } else {
      rpcClient.entityAttributeDetails(entityname.get, attribute.get).map(EntityDetailResponse(200, entityname.get, attribute.get, _))
    }

    if (res.isSuccess) {
      response.ok.json(res.get)
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
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

    val res = rpcClient.entityBenchmarkAndUpdateScanWeights(entityname.get, attribute.get)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
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

    val res = rpcClient.entitySparsify(entityname.get, attribute.get)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }

  /**
    *
    */
  get("/entity/vacuum") { request: Request =>
    val entityname = request.params.get("entityname")

    if (entityname.isEmpty) {
      response.ok.json(GeneralResponse(500, "entity/index not specified"))
    }

    val res = rpcClient.entityVacuum(entityname.get)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }

  /**
    *
    */
  get("/entity/drop") { request: Request =>
    val entityname = request.params.get("entityname")

    if (entityname.isEmpty) {
      response.ok.json(GeneralResponse(500, "entity/index not specified"))
    }

    val res = rpcClient.entityDrop(entityname.get)
    rpcClient.indexDrop(entityname.get)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
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

    val res = rpcClient.entityPreview(entityname.get)

    if (res.isSuccess) {
      response.ok.json(EntityReadResponse(200, entityname.get, res.get.map(_.results).head))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }


  /**
    *
    */
  post("/entity/add") { request: EntityCreateRequest =>
    val res = rpcClient.entityCreate(request.entityname, request.attributes.map(a => RPCAttributeDefinition(a.name, a.datatype, if(a.storagehandler == null){None} else {Some(a.storagehandler)}, params = a.params)))

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
    val res = rpcClient.entityGenerateRandomData(request.entityname, request.ntuples, request.ndims, 0, 0, 1, None)

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
    val res = rpcClient.entityCreateAllIndexes(request.entityname, request.attributes.map(_.name), 2)

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
    val res = rpcClient.entityPartition(request.entityname, request.npartitions, Some(request.attribute), request.materialize, request.replace)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200, res.get))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }

  /**
    *
    */
  post("/index/add") { request: IndexCreateRequest =>
    val res = rpcClient.indexCreate(request.entityname, request.attribute, request.indextype, request.norm, request.options)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }


  /**
    *
    */
  get("/index/details") { request: Request =>
    val indexname = request.params.get("indexname")

    if (indexname.isEmpty) {
      response.ok.json(GeneralResponse(500, "entity not specified"))
    }

    val details = rpcClient.indexDetails(indexname.get)

    if (details.isSuccess) {
      response.ok.json(IndexDetailResponse(200, indexname.get, details.get))
    } else {
      response.ok.json(GeneralResponse(500, details.failed.get.getMessage))
    }
  }


  /**
    *
    */
  post("/index/partition") { request: IndexPartitionRequest =>
    val res = rpcClient.indexPartition(request.indexname, request.npartitions, Some(request.attribute), request.materialize, request.replace)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200, res.get))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }


  /**
    *
    */
  post("/search/compound") { request: SearchRequest =>
    val res = rpcClient.doQuery(request.toRPCQueryObject)
    if (res.isSuccess) {
      response.ok.json(new SearchCompoundResponse(200, new SearchResponse(res.get)))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }


  /**
    *
    */
  post("/search/progressive") { request: SearchRequest =>
    val res = rpcClient.doProgressiveQuery(request.toRPCQueryObject, processProgressiveResults(request.id), completedProgressiveResults)

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


  private def processProgressiveResults(id: String)(res: Try[RPCQueryResults]): Unit = {
    if (res.isSuccess) {
      val results = res.get
      progTempResults.get(id).get += SearchProgressiveIntermediaryResponse(id, results.confidence, results.info.getOrElse("indextype", ""), results.time, results.results, ProgressiveQueryStatus.RUNNING)
    } else {
      log.error("error in progressive results processing", res.failed.get)
      completedProgressiveResults(id, res.failed.get.getMessage, ProgressiveQueryStatus.ERROR)
    }
  }


  val progTempResults = mutable.HashMap[String, mutable.Queue[SearchProgressiveIntermediaryResponse]]()

  private def completedProgressiveResults(id: String): Unit = {
    completedProgressiveResults(id, "", ProgressiveQueryStatus.FINISHED)
  }

  private def completedProgressiveResults(id: String, message: String, newStatus: ProgressiveQueryStatus.Value): Unit = {
    progTempResults.get(id).get += SearchProgressiveIntermediaryResponse(id, 0.0, message, 0, Seq(), newStatus)
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


  /**
    *
    */
  get("/storagehandlers/list") { request: Request =>
    val handlers = rpcClient.storageHandlerList()

    if (handlers.isSuccess) {
      response.ok.json(StorageHandlerResponse(200, handlers.get))
    } else {
      response.ok.json(StorageHandlerResponse(500, Map()))
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
