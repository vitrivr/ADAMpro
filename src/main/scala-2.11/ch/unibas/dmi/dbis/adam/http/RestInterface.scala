package ch.unibas.dmi.dbis.adam.http;


import akka.actor._
import akka.util.Timeout
import ch.unibas.dmi.dbis.adam.api._
import ch.unibas.dmi.dbis.adam.datatypes.Feature
import ch.unibas.dmi.dbis.adam.http.Protocol._
import ch.unibas.dmi.dbis.adam.query.{ProgressiveQueryStatus, Result}
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import org.apache.spark.sql.types._
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
    countRoute ~ createRoute ~ displayRoute ~ dropRoute ~ cacheRoute ~
      importRoute ~ importFileRoute ~ indexRoute ~ listRoute ~ indexQueryRoute ~
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
      post {
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
  private val importFileRoute : Route = pathPrefix("importfile") {
    path(Segment) { tablename =>
      put {
        ??? //TODO
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
  private val cacheRoute : Route = pathPrefix("cache") {
    path(Segment) { tablename =>
      post { requestContext =>
        val responder = createResponder(requestContext)
        CacheOp(tablename)
        responder ! AddedToCache
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
          val results = SequentialQueryOp(tablename, Feature.conv_str2vector(query.q), query.k, NormBasedDistanceFunction(query.norm))
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
          val responder = ctx.responder
          var nResponses = ProgressiveQueryOp(tablename, Feature.conv_str2vector(query.q), query.k, NormBasedDistanceFunction(query.norm), sendNextChunk)

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