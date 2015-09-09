package ch.unibas.dmi.dbis.adam.http

import ch.unibas.dmi.dbis.adam.query.Result
import org.json4s.{DefaultFormats, Formats}
import spray.httpx.Json4sSupport

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object Protocol extends Json4sSupport {
  override implicit def json4sFormats: Formats = DefaultFormats

  //case class Operation(opName: String, options : Map[String, String])

  case class CountResult(count : Long)
  case class DisplayResult(tuples : Seq[(Long, String)])

  case object TableCreated
  case object TableDropped
  case object TableImported

  case object IndexCreated

  case class ListTables(tables : Seq[String])

  case class IndexQuery(k : Int, q : String, norm : Int = 1)
  case class SeqQuery(k : Int, q : String, norm : Int = 1)
  case class ProgQuery(k : Int, q : String, norm : Int = 1)

  case class QueryResults(tuples : Seq[Result])

  //Exceptions
  case object IndexNotExistingException
  case object TableExistingException
  case object TableNotExistingException
}
