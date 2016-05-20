package ch.unibas.dmi.dbis.adam.query.query

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.index.Index.PartitionID
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * November 2015
  */
abstract class Query(queryID: Option[String] = Some(java.util.UUID.randomUUID().toString)) extends Serializable {}

/**
  * Boolean query parameters.
  *
  * @param where a where 'clause' in form of (String, String), if the first string ends with '!=' or 'IN' the
  *              operator is used in the query, otherwise a '=' is added in between; AND-ing is assumed
  * @param join  Seq of (table, columns to join on)
  * @param queryID
  */
case class BooleanQuery(
                         where: Option[Seq[(String, String)]] = None,
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
  def buildWhereClause(): String = {
    //TODO: refactor
    val regex = s"""^(${sqlOperators.mkString("|")}){0,1}(.*)""".r

    where.get.map { case (field, value) =>
      val regex(prefix, suffix) = value

      //if a sqlOperator was found in the value field then keep the SQL operator, otherwise add a '='
      (prefix, suffix) match {
        case (null, s) => field + " = " + " '" + value + "'"
        case _ => field + " " + value
      }
    }.mkString("(", ") AND (", ")")
  }

  override def hashCode(): Int = {
    where.hashCode() + join.hashCode()
  }

  override def equals(obj: scala.Any): Boolean = {
    if (obj.isInstanceOf[BooleanQuery]) {
      val other = obj.asInstanceOf[BooleanQuery]
      where.equals(other.where) && join.equals(other.join)
    } else {
      false
    }
  }
}


/**
  * Nearest neighbour query parameters.
  *
  * @param column    name of column to perform query on
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
                                  column: String,
                                  q: FeatureVector,
                                  distance: DistanceFunction,
                                  k: Int,
                                  indexOnly: Boolean = false,
                                  options: Map[String, String] = Map[String, String](),
                                  partitions: Option[Set[PartitionID]] = None,
                                  queryID: Option[String] = Some(java.util.UUID.randomUUID().toString))
  extends Query(queryID) {}

/**
  *
  * @param tidFilter
  * @tparam A
  */
case class PrimaryKeyFilter[A](tidFilter: Set[A]) {
  def this(filter: DataFrame) = {
    this(filter.collect().map(_.getAs[A](0)).toSet)
  }

  def +:(filter: Option[DataFrame]): PrimaryKeyFilter[A] = {
    if (filter.isDefined) {
      PrimaryKeyFilter(tidFilter ++ filter.get.collect().map(_.getAs[A](0)))
    } else {
      this
    }
  }
}


