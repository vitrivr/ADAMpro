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


case class BooleanQuery(
                         where: Map[String, String],
                         join : List[(String, List[String])], //table, columns
                         queryID: Option[String] = Some(java.util.UUID.randomUUID().toString))
  extends Query(queryID) {
  def getWhereClause() : String = where.map(c => c._1 + " = " + c._2).mkString(" AND ")
}


/**
 *
 * @param q
 * @param distance
 * @param k
 * @param indexOnly
 * @param options
 * @param queryID
 */
case class NearestNeighbourQuery(
                                  q: FeatureVector,
                                  distance: DistanceFunction,
                                  k: Int,
                                  indexOnly : Boolean = false,
                                  options : Map[String, String] = Map[String, String](),
                                  queryID : Option[String] = Some(java.util.UUID.randomUUID().toString))
  extends Query(queryID) {}