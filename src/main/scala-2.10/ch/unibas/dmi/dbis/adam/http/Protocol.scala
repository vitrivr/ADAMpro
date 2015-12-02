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

  case object EntityCreated
  case object EntityDropped

  case object IndexCreated

  case object AddedToCache

  case class ListEntities(tables : Seq[String])
  case class EntityPreview(entries : Seq[String])

  case class ImportPath(path : String)
  case object EntityImported


  case class IndexQuery(k : Int, q : String, norm : Int = 1)
  case class SeqQuery(k : Int, q : String, norm : Int = 1)
  case class ProgQuery(k : Int, q : String, norm : Int = 1)

  case class QueryResults(tuples : Seq[Result])

  case object EvaluationStarted


  //Exceptions
  case object IndexNotExistingException
  case object EntityExistingException
  case object EntityNotExistingException
}
