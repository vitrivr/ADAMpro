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
                         where: Map[String, String],
                         join: Seq[(String, Seq[String])],
                         queryID: Option[String] = Some(java.util.UUID.randomUUID().toString))
  extends Query(queryID) {
  def getWhereClause(): String = where.map(c => c._1 + " = " + c._2).mkString(" AND ")
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