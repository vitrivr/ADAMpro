package ch.unibas.dmi.dbis.adam.main

import akka.actor._
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.http.RestInterface
import spray.can.Http

import scala.concurrent.duration._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class RESTStartup(config : AdamConfig) extends Runnable {
  def run() : Unit = {
    //TODO: get from config
    val host = "localhost"
    val port = 8080

    implicit val system = ActorSystem("adam-management-service")

    val api = system.actorOf(Props(new RestInterface()), "httpInterface")

    implicit val executionContext = system.dispatcher
    implicit val timeout = Timeout(10 seconds)

    IO(Http).ask(Http.Bind(listener = api, interface = host, port = port))
      .mapTo[Http.Event]
      .map {
      case Http.Bound(address) =>
        println(s"REST interface bound to $address")
      case Http.CommandFailed(cmd) =>
        println("REST interface could not bind to " +
          s"$host:$port, ${cmd.failureMessage}")
        system.shutdown()
    }
  }
}
