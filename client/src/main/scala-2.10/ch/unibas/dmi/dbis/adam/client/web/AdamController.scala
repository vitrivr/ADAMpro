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
import scala.util.Success

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

  case class GeneralResponse(code: Int, message: String = "")


  /**
    *
    */
  get("/entity/list") { request: Request =>
    val filter = request.params.get("filter")

    val entities = if (filter.isEmpty) {
      rpcClient.listEntities()
    } else {
      Success(Seq(filter.get))
    }

    if (entities.isSuccess) {
      response.ok.json(EntityListResponse(200, entities.get))
    } else {
      response.ok.json(GeneralResponse(500, entities.failed.get.getMessage))
    }
  }

  case class EntityListResponse(code: Int, entities: Seq[String])

  /**
    *
    */
  get("/entity/details") { request: Request =>
    val entityname = request.params.get("entityname")

    if (entityname.isEmpty) {
      response.ok.json(GeneralResponse(500, "entity not specified"))
    }

    val details = rpcClient.getDetails(entityname.get)

    if (details.isSuccess) {
      response.ok.json(EntityDetailResponse(200, entityname.get, details.get))
    } else {
      response.ok.json(GeneralResponse(500, details.failed.get.getMessage))
    }
  }

  case class EntityDetailResponse(code: Int, entity : String, details : Map[String, String])

  /**
    *
    */
  post("/entity/add") { request: EntityCreationRequest =>
    val res = rpcClient.createEntity(request.entityname, request.fields)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200, res.get))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }

  /**
    *
    */
  post("/entity/insertdemo") { request: InsertDataRequest =>
    val res = rpcClient.prepareDemo(request.entityname, request.ntuples, request.ndims, request.fields)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }

  /**
    *
    */
  post("/entity/indexall") { request: IndexAllRequest =>
    val res = rpcClient.addAllIndex(request.entityname, request.fields, 2)


    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }

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

    val res = rpcClient.addIndex(request.entityname, request.column, indextype, request.norm, request.options)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }


  /**
    *
    */
  post("/index/repartition") { request: RepartitionRequest =>
    val res = rpcClient.repartition(request.indexname, request.partitions, request.columns.filter(_.length > 0), request.materialize, request.replace)

    if (res.isSuccess) {
      response.ok.json(GeneralResponse(200, res.get))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }

  /**
    *
    */
  post("/query/compound") { request: CompoundQueryRequest =>
    val res = rpcClient.compoundQuery(request)
    if (res.isSuccess) {
      response.ok.json(CompoundQueryResponse(200, res.get))
    } else {
      response.ok.json(GeneralResponse(500, res.failed.get.getMessage))
    }
  }

  case class CompoundQueryResponse(code: Int, details: CompoundQueryDetails)


  /**
    *
    */
  post("/query/progressive") { request: ProgressiveQueryRequest =>
    val res = rpcClient.progressiveQuery(request.id, request.entityname, request.q, request.column, request.hints, request.k, processProgressiveResults, completedProgressiveResults)

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

  private def processProgressiveResults(id: String, confidence: Double, source: String, time: Long, results: Seq[(Float, Map[String, String])]): Unit = {
    val sourcetype = if(source.length > 0){
      source.substring(0, source.indexOf("(")).toLowerCase.trim
    } else {
      ""
    }
    progTempResults.get(id).get += ProgressiveTempResponse(id, confidence, source, sourcetype, time, results, ProgressiveQueryStatus.RUNNING)
  }

  case class ProgressiveTempResponse(id: String, confidence: Double, source: String, sourcetype: String, time: Long, results: Seq[(Float, Map[String, String])], status: ProgressiveQueryStatus.Value)

  val progTempResults = mutable.HashMap[String, mutable.Queue[ProgressiveTempResponse]]()

  private def completedProgressiveResults(id: String): Unit = {
    progTempResults.get(id).get += ProgressiveTempResponse(id, 0.0, "", "", 0, Seq(), ProgressiveQueryStatus.FINISHED)
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
