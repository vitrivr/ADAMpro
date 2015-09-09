package ch.unibas.dmi.dbis.adam.http;


import akka.actor._
import akka.util.Timeout
import ch.unibas.dmi.dbis.adam.api._
import ch.unibas.dmi.dbis.adam.data.types.Feature
import ch.unibas.dmi.dbis.adam.http.Protocol._
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import org.apache.spark.sql.types._
import spray.can.Http
import spray.http._
import spray.routing._

import scala.concurrent.duration._
import scala.language.postfixOps

class RestInterface extends HttpServiceActor with RestApi {
  def receive = runRoute(routes)
}

trait RestApi extends HttpService with ActorLogging { actor: Actor =>
  import ch.unibas.dmi.dbis.adam.http.Protocol._

  implicit val timeout = Timeout(10 seconds)

  def routes: Route =
    countRoute ~ createRoute ~ displayRoute ~ dropRoute ~ importRoute ~ indexRoute ~ listRoute ~ indexQueryRoute ~ seqQueryRoute

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
        val schema = StructType(
          List(
            StructField("id", LongType, false),
            StructField("feature", ArrayType(FloatType), false)
          )
        )
        val result = CreateOp(tablename, schema)
        responder ! TableCreated
      }
    }
  }

  /**
   *
   */
  private val displayRoute : Route = pathPrefix("display") {
    path(Segment) { tablename =>
      post { requestContext =>
        val responder = createResponder(requestContext)
        val results = DisplayOp(tablename)

        responder ! DisplayResult(results.map(result => (result._1, result._2.mkString("<", ",", ">"))))
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
  private val importRoute : Route = pathPrefix("import") {
    path(Segment) { tablename =>
      put {
        entity(as[String]) { csv => requestContext =>
          val responder = createResponder(requestContext)
          ImportOp(tablename, csv.split("\n"))
          responder ! TableImported
        }
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
          IndexOp(tablename, indextypename, Map[String, String]())
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
    path(Segment) { indexname =>
        post {
          entity(as[IndexQuery]) { query => requestContext =>
            val responder = createResponder(requestContext)
            val results = IndexQueryOp(indexname, Feature.conv_str2vector(query.q), query.k, NormBasedDistanceFunction(query.norm))
            responder ! QueryResults(results)
          }
        }
      }
   }


  /**
   *
   */
  private val seqQueryRoute : Route = pathPrefix("seqquery") {
    path(Segment) { tablename =>
      post {
        entity(as[SeqQuery]) { query => requestContext =>
          val responder = createResponder(requestContext)
          val results = SeqQueryOp(tablename, Feature.conv_str2vector(query.q), query.k, NormBasedDistanceFunction(query.norm))
          responder ! QueryResults(results)
        }
      }
    }
  }

  /**
   *
   */
  private val progQueryRoute : Route = pathPrefix("progquery") {
    path(Segment) { tablename =>
      post {
        entity(as[ProgQuery]) { query =>
          streamProgResponse(tablename, query)
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
  private def streamProgResponse(tablename : TableName, query : ProgQuery)(ctx: RequestContext): Unit =
    actorRefFactory.actorOf {
      Props {
        new Actor with ActorLogging {
          var nResponses = ProgQueryOp(tablename, Feature.conv_str2vector(query.q), query.k, NormBasedDistanceFunction(query.norm), nextChunkReady)

          ctx.responder ! ChunkedResponseStart(HttpResponse()).withAck(Ok(nResponses))

          def nextChunkReady(res : Seq[Result]) : Unit = {
            ctx.responder ! MessageChunk(res.mkString(",")).withAck(nResponses)
            nResponses = nResponses - 1
          }

          def receive = {
            case Ok(0) =>
              ctx.responder ! ChunkedMessageEnd
              context.stop(self)

            case ev: Http.ConnectionClosed =>
              log.warning("Stopping response streaming due to {}", ev)
          }
        }
      }
    }


  /**
   *
   * @param requestContext
   * @return
   */
  private def createResponder(requestContext:RequestContext) = {
    context.actorOf(Props(new Responder(requestContext)))
  }
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