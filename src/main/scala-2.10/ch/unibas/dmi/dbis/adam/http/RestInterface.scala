package ch.unibas.dmi.dbis.adam.http

import akka.actor._
import akka.util.Timeout
import ch.unibas.dmi.dbis.adam.api._
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.http.Protocol._
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.MinkowskiDistance
import ch.unibas.dmi.dbis.adam.query.progressive.ProgressiveQueryStatus
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import spray.can.Http
import spray.http._
import spray.routing.{RequestContext, _}

import scala.concurrent.duration._
import scala.language.postfixOps

//TODO: switch REST code to akka-http
class RestInterface extends HttpServiceActor with RestApi {
  def receive = runRoute(routes)
}

trait RestApi extends HttpService with ActorLogging {
  actor: Actor =>

  private implicit val requestTimeout = Timeout(100 seconds)

  def routes: Route =
    countRoute ~ createRoute ~ generateDataRoute ~ dropRoute ~
      indexRoute ~ listRoute ~ indexQueryRoute ~
      seqQueryRoute ~ progQueryRoute ~
      importRoute ~  previewRoute ~
      generateRoute ~ evaluationRoute

  /**
   *
   */
  private def countRoute: Route = pathPrefix("count" / Segment) { entityname =>
    get { requestContext =>
      val responder = createResponder(requestContext)
      val result = CountOp(entityname)
      responder ! CountResult(result)
    }
  }

  /**
   *
   */
  private def createRoute: Route = pathPrefix("create" / Segment) { entityname =>
    get { requestContext =>
      val responder = createResponder(requestContext)
      val result = CreateOp(entityname)
      responder ! EntityCreated
    }
  }

  /**
   *
   */
  private def generateDataRoute: Route = pathPrefix("generate" / Segment / Segment / Segment) { (entityname, numberOfElements, numberOfDimensions) =>
    get { requestContext =>
      val responder = createResponder(requestContext)
      GenerateDataOp(entityname, numberOfElements.toInt, numberOfDimensions.toInt)
      responder ! EntityImported
    }
  }

  /**
   *
   */
  private def dropRoute: Route = pathPrefix("drop" / Segment) { entityname =>
    get { requestContext =>
      val responder = createResponder(requestContext)
      DropOp(entityname)
      responder ! EntityDropped
    }
  }


  /**
   *
   */
  private def indexRoute: Route = pathPrefix("index" / Segment / Segment) { (entityname, indextypename) =>

    get { requestContext =>
      val responder = createResponder(requestContext)
      IndexOp(entityname, indextypename, new MinkowskiDistance(1), Map[String, String]())
      responder ! IndexCreated
    }
  }

  /**
   *
   */
  private def listRoute: Route = pathPrefix("list") {
    get { requestContext =>
      val responder = createResponder(requestContext)
      val results = ListOp()
      responder ! ListEntities(results)
    }
  }

  /**
   *
   */
  private def indexQueryRoute: Route = pathPrefix("indexquery" / Segment) { indexname =>
    post {
      entity(as[IndexQuery]) { queryobj => requestContext =>
        val responder = createResponder(requestContext)

        val query = Feature.conv_str2vector(queryobj.q)
        val k = queryobj.k
        val norm = 1
        val withMetadata = true

        val nnq = NearestNeighbourQuery(query, new MinkowskiDistance(norm), k)
        val bq = None

        val results = QueryOp.index(indexname, nnq, bq, withMetadata)
        responder ! QueryResults(results)
      }
    }
  }


  /**
   *
   */
  private def seqQueryRoute: Route = pathPrefix("seqquery" / Segment) { entityname =>
    post {
      entity(as[IndexQuery]) { queryobj => requestContext =>
        val responder = createResponder(requestContext)

        val query = Feature.conv_str2vector(queryobj.q)
        val k = queryobj.k
        val norm = 1
        val withMetadata = false

        val nnq = NearestNeighbourQuery(query, new MinkowskiDistance(norm), k)
        val bq = None

        val results = QueryOp.sequential(entityname, nnq, bq, withMetadata)
        responder ! QueryResults(results)
      }
    }
  }

  /**
   *
   */
  private def progQueryRoute: Route = pathPrefix("progquery" / Segment) { entityname =>
    post {
      entity(as[ProgQuery]) { queryobj =>
        streamProgResponse(entityname, queryobj)
      }
    }
  }

  /**
   *
   * @param remaining
   */
  case class Ok(remaining: Int)

  /**
   *
   * @param ctx
   */
  private def streamProgResponse(ename: EntityName, queryobj: ProgQuery)(ctx: RequestContext): Unit =
    actorRefFactory.actorOf {
      Props {
        new Actor with ActorLogging {
          val responder = ctx.responder

          val entityname = ename
          val query = Feature.conv_str2vector(queryobj.q)
          val k = queryobj.k
          val norm = queryobj.norm
          val withMetadata = true

          val nnq = NearestNeighbourQuery(query, new MinkowskiDistance(norm), k)
          val bq = None

          val tracker = QueryOp.progressive(entityname, nnq, bq, sendNextChunk, withMetadata) //TODO: change this as progressive might still have to be blocking
          var nResponses = tracker.numberOfFutures

          //start
          responder ! ChunkedResponseStart(HttpResponse()).withAck(Ok(nResponses))

          //next chunk
          def sendNextChunk(status: ProgressiveQueryStatus.Value, res: Seq[Result], confidence: Float, details: Map[String, String]): Unit = synchronized {
            nResponses = nResponses - 1
            responder ! MessageChunk(res.mkString(",")).withAck(Ok(nResponses)) //TODO: reformat result
          }

          def receive = {
            case Ok(0) => //finished streaming, close
              responder ! ChunkedMessageEnd
              context.stop(self)
            case Ok(remaining) =>
            case ev: Http.ConnectionClosed =>
              log.warning("Stopping response streaming due to {}", ev)
          }
        }
      }
    }


  private def importRoute: Route = pathPrefix("import" / Segment / Segment) { (entityname, filename) =>
    get { requestContext =>
      val responder = createResponder(requestContext)
      ImportOp(entityname, "hdfs://original/" + filename)
      responder ! EntityImported
    }
  }

  private def previewRoute: Route = pathPrefix("preview" / Segment) { entityname =>
    get { requestContext =>
      val responder = createResponder(requestContext)
      val results = PreviewOp(entityname)
      responder ! EntityPreview(results)
    }
  }

  private def generateRoute: Route = pathPrefix("generate" / Segment / Segment) { (numberOfElements, numberOfDimensions) =>
    get { requestContext =>
      val responder = createResponder(requestContext)
      EvaluationOp.generate()
    }
  }

  private def evaluationRoute: Route = pathPrefix("evaluation") {
    get { requestContext =>
      val responder = createResponder(requestContext)
      EvaluationOp.perform()
    }
  }


  private def createResponder(requestContext: RequestContext) = context.actorOf(Props(new Responder(requestContext)))
}

class Responder(requestContext: RequestContext) extends Actor with ActorLogging {
  /**
   *
   * @return
   */
  def receive = {
    case result: CountResult =>
      requestContext.complete(StatusCodes.OK, result)

    case EntityCreated =>
      requestContext.complete(StatusCodes.OK)
      killYourself

    case EntityDropped =>
      requestContext.complete(StatusCodes.OK)
      killYourself

    case EntityImported =>
      requestContext.complete(StatusCodes.OK)
      killYourself

    case IndexCreated =>
      requestContext.complete(StatusCodes.OK)
      killYourself

    case result: ListEntities =>
      requestContext.complete(StatusCodes.OK, result)
      killYourself

    case result: QueryResults =>
      requestContext.complete(StatusCodes.OK, result)
      killYourself

    case EvaluationStarted =>
      requestContext.complete(StatusCodes.OK)
      killYourself

    case _ =>
      requestContext.complete(StatusCodes.OK)
      killYourself
  }

  /**
   *
   */
  private def killYourself = self ! PoisonPill
}