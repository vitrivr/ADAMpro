package ch.unibas.dmi.dbis.adam.query.query

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction

/**
  * adamtwo
  *
  * Ivan Giangreco
  * November 2015
  */
abstract class Query(queryID: Option[String] = Some(java.util.UUID.randomUUID().toString)) {}

/**
  * Boolean query parameters.
  *
  * @param where
  * @param join Seq of (table, columns to join on)
  * @param queryID
  */
case class BooleanQuery(
                         where: Map[String, String], //TODO: possibly switch map to seq
                         join: Option[Seq[(String, Seq[String])]] = None,
                         queryID: Option[String] = Some(java.util.UUID.randomUUID().toString))
  extends Query(queryID) {


  /**
    * List of SQL operators to keep in query (otherwise a '=' is added)
    */
  val sqlOperators = Seq("!=", "IN")


  /**
    * Builds a where clause.
    *
    * @return
    */
  def getWhereClause(): String = {
    val regex = s"""^(${sqlOperators.mkString("|")}){0,1}(.*)""".r

    where.map { case (field, value) =>
      val regex(prefix,suffix) = value

      //if a sqlOperator was found then keep the SQL operator, otherwise add a '='
      (prefix, suffix) match {
        case(null, s) => field + " =" + " " + value
        case _ => field + " " + value
      }
    }.mkString("(", ") AND (", ")")
  }
}


/**
  * Nearest neighbour query parameters.
  *
  * @param q
  * @param distance
  * @param k
  * @param indexOnly if set to true, then only the index is scanned and the results are result candidates only
  *                  and may contain false positives
  * @param options
  * @param queryID
  */
case class NearestNeighbourQuery(
                                  q: FeatureVector,
                                  distance: DistanceFunction,
                                  k: Int,
                                  indexOnly: Boolean = false,
                                  options: Map[String, String] = Map[String, String](),
                                  queryID: Option[String] = Some(java.util.UUID.randomUUID().toString))
  extends Query(queryID) {}