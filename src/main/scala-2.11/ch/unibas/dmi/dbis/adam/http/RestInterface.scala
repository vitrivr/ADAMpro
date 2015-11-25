package ch.unibas.dmi.dbis.adam.http;


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

class RestInterface extends HttpServiceActor with RestApi {
  def receive = runRoute(routes)
}

trait RestApi extends HttpService with ActorLogging { actor: Actor =>
  import ch.unibas.dmi.dbis.adam.http.Protocol._

  private implicit val requestTimeout = Timeout(100 seconds)

  def routes: Route =
    countRoute ~ createRoute ~ dropRoute ~
      indexRoute ~ listRoute ~ indexQueryRoute ~
      seqQueryRoute ~ progQueryRoute

  /**
   *
   */
  private val countRoute : Route = pathPrefix("count") {
    path(Segment) { tablename =>
      post { requestContext =>
        val responder = createResponder(requestContext)
        val result = CountOp(tablename)
        responder ! CountResult(result)
      }
    }
  }

  /**
   *
   */
  private val createRoute : Route = pathPrefix("create") {
    path(Segment) { tablename =>
      post { requestContext =>
        val responder = createResponder(requestContext)
        val result = CreateOp(tablename)
        responder ! TableCreated
      }
    }
  }

  /**
   *
   */
  private val dropRoute : Route = pathPrefix("drop") {
    path(Segment) { tablename =>
      post { requestContext =>
        val responder = createResponder(requestContext)
        DropOp(tablename)
        responder ! TableDropped
      }
    }
  }


  /**
   *
   */
  private val indexRoute : Route = pathPrefix("index") {
    path(Segment) { tablename =>
      path(Segment) { indextypename =>
        post { requestContext =>
          val responder = createResponder(requestContext)
          IndexOp(tablename, indextypename, new MinkowskiDistance(1), Map[String, String]())
          responder ! IndexCreated
        }
      }
    }
  }

  /**
   *
   */
  private val listRoute : Route = pathPrefix("list") {
    pathEnd {
      post { requestContext =>
        val responder = createResponder(requestContext)
        val results = ListOp()
        responder ! ListTables(results)
      }
    }
  }

  /**
   *
   */
  private val indexQueryRoute : Route = pathPrefix("indexquery") {
    path(Segment) { iname =>
        post {
          entity(as[IndexQuery]) { queryobj => requestContext =>
            val responder = createResponder(requestContext)

            val indexname = iname
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
   }


  /**
   *
   */
  private val seqQueryRoute : Route = pathPrefix("seqquery") {
    path(Segment) { ename =>
      post {
        entity(as[IndexQuery]) { queryobj => requestContext =>
          val responder = createResponder(requestContext)

          val entityname = ename
          val query = Feature.conv_str2vector(queryobj.q)
          val k = queryobj.k
          val norm = 1
          val withMetadata = true

          val nnq = NearestNeighbourQuery(query, new MinkowskiDistance(norm), k)
          val bq = None

          val results = QueryOp.sequential(entityname, nnq, bq, withMetadata)

          responder ! QueryResults(results)
        }
      }
    }
  }

  /**
   *
   */
  private val progQueryRoute : Route = pathPrefix("progquery") {
    path(Segment) { entityname =>
      post {
        entity(as[ProgQuery]) { queryobj =>
          streamProgResponse(entityname, queryobj)
        }
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
  private def streamProgResponse(ename : EntityName, queryobj : ProgQuery)(ctx: RequestContext): Unit =
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

          var nResponses = QueryOp.progressive(entityname, nnq, bq, sendNextChunk, withMetadata)

          //start
          responder ! ChunkedResponseStart(HttpResponse()).withAck(Ok(nResponses))

          //next chunk
          def sendNextChunk(status : ProgressiveQueryStatus.Value, res : Seq[Result], confidence : Float, details : Map[String, String]) : Unit = synchronized {
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


  private def createResponder(requestContext:RequestContext) = context.actorOf(Props(new Responder(requestContext)))
}

class Responder(requestContext:RequestContext) extends Actor with ActorLogging {
  /**
   *
   * @return
   */
  def receive = {
    case result : CountResult =>
      requestContext.complete(StatusCodes.OK, result)

    case TableCreated =>
      requestContext.complete(StatusCodes.OK)
      killYourself

    case TableDropped =>
      requestContext.complete(StatusCodes.OK)
      killYourself

    case TableImported =>
      requestContext.complete(StatusCodes.OK)
      killYourself

    case IndexCreated =>
      requestContext.complete(StatusCodes.OK)
      killYourself

    case result : ListTables =>
      requestContext.complete(StatusCodes.OK, result)
      killYourself

    case result : QueryResults =>
      requestContext.complete(StatusCodes.OK, result)
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