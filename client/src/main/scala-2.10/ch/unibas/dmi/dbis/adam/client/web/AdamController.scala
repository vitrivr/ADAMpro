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
    log.info("listing data")
    val results = rpcClient.listEntities().map(entity => EntityDetailResponse(entity, rpcClient.getDetails(entity)))

    response.ok.json(EntityListResponse(200, results))
  }

  case class EntityListResponse(code: Int, entities: Seq[EntityDetailResponse])

  case class EntityDetailResponse(entityname: String, details: Map[String, String])

  /**
    *
    */
  post("/entity/add") { request: PreparationRequest =>
    val res = rpcClient.prepareDemo(request.entityname, request.ntuples, request.ndims, request.fields)

    log.info("prepared data")

    if (res) {
      response.ok.json(PreparationRequestResponse(200, request.entityname, request.ntuples, request.ndims))
    } else {
      response.ok.json(PreparationRequestResponse(500))
    }
  }

  case class PreparationRequestResponse(code: Int, entityname: String = "", ntuples: Int = 0, ndims: Int = 0)


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

  case class IndexRequestResponse(code: Int, indexname: String = "")


  /**
    *
    */
  post("/index/repartition") { request: RepartitionRequest =>
    val res = rpcClient.repartition(request.indexname, request.partitions, request.usemetadata, request.columns)

    log.info("repartitioned")

    if (!res.isEmpty) {
      response.ok.json(IndexRequestResponse(200, res))
    } else {
      response.ok.json(IndexRequestResponse(500))
    }
  }

  /**
    *
    */
  post("/query/compound") { request: CompoundQueryRequest =>
    log.info("query: " + request)
    val res = rpcClient.compoundQuery(request)
    response.ok.json(res)
  }


  /**
    *
    */
  post("/query/progressive") { request: ProgressiveQueryRequest =>
    log.info("progressive query start: " + request)
    val res = rpcClient.progressiveQuery(request.id, request.entityname, request.q, request.hints, request.k, processProgressiveResults, completedProgressiveResults)

    progTempResults.synchronized {
      if (!progTempResults.contains(request.id)) {
        val queue = mutable.Queue[ProgressiveTempResponse]()
        progTempResults.put(request.id, queue)
      } else {
        log.error("query id is already being used")
      }
    }

    response.ok.json(ProgressiveStartResponse(request.id))
  }

  case class ProgressiveStartResponse(id: String)


  private def processProgressiveResults(id: String, confidence: Double, source: String, time: Long, results: Seq[(Float, Long)]): Unit = {
    val sourcetype = source.substring(0, source.indexOf("(")).toLowerCase
    progTempResults.get(id).get += ProgressiveTempResponse(id, confidence, source, sourcetype, time, results, ProgressiveQueryStatus.RUNNING)
  }

  case class ProgressiveTempResponse(id: String, confidence: Double, source : String, sourcetype: String, time: Long, results: Seq[(Float, Long)], status: ProgressiveQueryStatus.Value)

  val progTempResults = mutable.HashMap[String, mutable.Queue[ProgressiveTempResponse]]()

  private def completedProgressiveResults(id: String): Unit = {
    progTempResults.get(id).get += ProgressiveTempResponse(id, 0.0, "", "", 0, Seq(), ProgressiveQueryStatus.FINISHED)
    log.info("completed progressive query, deleting results in 10 seconds")
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
  post("/query/progressive/temp") { request: ProgressiveQueryTemporaryResultsRequest =>
    log.info("progressive query temporary results: " + request)

    progTempResults.synchronized {
      if (progTempResults.get(request.id).isDefined && !progTempResults.get(request.id).get.isEmpty) {
        val result = progTempResults.get(request.id).get.dequeue()
        response.ok.json(ProgressiveQueryResponse(result, result.status.toString))
      } else {
        response.ok
      }
    }
  }

  case class ProgressiveQueryResponse(results: ProgressiveTempResponse, status: String)

}

/**
  *
  */
object ProgressiveQueryStatus extends Enumeration {
  val RUNNING = Value("running")
  val PREMATURE_FINISHED = Value("premature")
  val FINISHED = Value("finished")
}
