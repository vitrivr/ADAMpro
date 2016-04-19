package ch.unibas.dmi.dbis.adam.query.query

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.storage.partitions.PartitionHandler._

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
  * @param where     a where 'clause' in form of (String, String), if the first string ends with '!=' or 'IN' the
  *                  operator is used in the query, otherwise a '=' is added in between; AND-ing is assumed
  * @param join      Seq of (table, columns to join on)
  * @param tidFilter additional filter (is applied to results of where condition if it is set)
  * @param queryID
  */
case class BooleanQuery(
                         where: Option[Seq[(String, String)]] = None,
                         join: Option[Seq[(String, Seq[String])]] = None,
                         var tidFilter: Option[Set[TupleID]] = None,
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
  def buildWhereClause(): String = {
    val regex = s"""^(${sqlOperators.mkString("|")}){0,1}(.*)""".r

    where.get.map { case (field, value) =>
      val regex(prefix, suffix) = value

      //if a sqlOperator was found then keep the SQL operator, otherwise add a '='
      (prefix, suffix) match {
        case (null, s) => field + " =" + " " + value
        case _ => field + " " + value
      }
    }.mkString("(", ") AND (", ")")
  }

  /**
    *
    * @param filter
    */
  def append(filter: Set[TupleID]): Unit = {
    tidFilter = Option(tidFilter.getOrElse(Set()) ++ filter)
  }

  override def hashCode(): Int = {
    where.hashCode() + join.hashCode() + tidFilter.hashCode()
  }

  override def equals(obj: scala.Any): Boolean = {
    if(obj.isInstanceOf[BooleanQuery]){
      val other = obj.asInstanceOf[BooleanQuery]
      where.equals(other.where) && join.equals(other.join) && tidFilter.equals(other.tidFilter)
    } else {
      false
    }
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
  * @param partitions
  * @param options
  * @param queryID
  */
case class NearestNeighbourQuery(
                                  q: FeatureVector,
                                  distance: DistanceFunction,
                                  k: Int,
                                  indexOnly: Boolean = false,
                                  options: Map[String, String] = Map[String, String](),
                                  partitions : Option[Set[PartitionID]] = None,
                                  queryID: Option[String] = Some(java.util.UUID.randomUUID().toString))
  extends Query(queryID) {}