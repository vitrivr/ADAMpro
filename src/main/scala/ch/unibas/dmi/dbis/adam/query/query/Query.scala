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
    val regex =
      s"""^(${sqlOperators.mkString("|")}){0,1}(.*)""".r

    where.get.map { case (field, value) =>
      val regex(prefix, suffix) = value

      //if a sqlOperator was found in the value field then keep the SQL operator, otherwise add a '='
      (prefix, suffix) match {
        case (null, s) => field + " = " + " '" + value + "'"
        case _ => field + " " + value
      }
    }.mkString("(", ") AND (", ")")
  }

  override def equals(that: Any): Boolean = {
    that match {
      case that: BooleanQuery =>
        this.where.equals(that.where) && this.join.equals(that.join)
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + where.hashCode()
    result = prime * result + join.hashCode()
    result
  }
}


/**
  * Nearest neighbour query parameters.
  *
  * @param column     name of column to perform query on
  * @param q          query vector
  * @param distance   distance function
  * @param k          number of elements to retrieve
  * @param indexOnly  if set to true, then only the index is scanned and the results are result candidates only
  *                   and may contain false positives
  * @param partitions partitions to query (if not set all partitions are queried)
  * @param options    options to pass to handler
  */
case class NearestNeighbourQuery(
                                  column: String,
                                  q: FeatureVector,
                                  weights: Option[FeatureVector],
                                  distance: DistanceFunction,
                                  k: Int,
                                  indexOnly: Boolean = false,
                                  options: Map[String, String] = Map[String, String](),
                                  partitions: Option[Set[PartitionID]] = None,
                                  queryID: Option[String] = Some(java.util.UUID.randomUUID().toString))
  extends Query(queryID) {

  override def equals(that: Any): Boolean = {
    that match {
      case that: NearestNeighbourQuery =>
        this.column.equals(that.column) &&
        this.q.equals(that.q) &&
        this.weights.isDefined == that.weights.isDefined &&
        this.weights.map(w1 => that.weights.map(w2 => w1.equals(w2)).getOrElse(false)).getOrElse(true)
        this.distance.getClass.equals(that.distance.getClass) &&
        this.k == that.k &&
        this.indexOnly == that.indexOnly &&
        this.partitions.isDefined == that.partitions.isDefined &&
        this.partitions.map(p1 => that.partitions.map(p2 => p1.equals(p2)).getOrElse(false)).getOrElse(true)
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + column.hashCode
    result = prime * result + q.hashCode()
    result = prime * result + weights.map(_.hashCode()).getOrElse(0)
    result = prime * result + distance.getClass.hashCode()
    result = prime * result + k
    result = prime * result + indexOnly.hashCode()
    result = prime * result + partitions.map(_.hashCode()).getOrElse(0)
    result
  }


}

/**
  *
  * @param filter id filter
  */
case class PrimaryKeyFilter[A](filter: DataFrame) {
  def +:(newFilter: Option[DataFrame]): PrimaryKeyFilter[A] = {
    if (newFilter.isDefined) {
      import org.apache.spark.sql.functions.col
      val fields = filter.schema.fieldNames.intersect(newFilter.get.schema.fieldNames)
      new PrimaryKeyFilter(filter.select(fields.map(col): _*).unionAll(newFilter.get.select(fields.map(col): _*)))
    } else {
      this
    }
  }
}


