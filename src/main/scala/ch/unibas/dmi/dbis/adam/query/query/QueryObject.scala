package ch.unibas.dmi.dbis.adam.query.query

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.helpers.partition.Partitioning.PartitionID
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction

/**
  * adamtwo
  *
  * Ivan Giangreco
  * November 2015
  */
//TODO: use query class
class Query(qo: Seq[QueryObject], queryID: Option[String] = Some(java.util.UUID.randomUUID().toString)) extends Serializable

abstract class QueryObject(queryID: Option[String] = Some(java.util.UUID.randomUUID().toString)) extends Serializable {}

/**
  * Boolean query parameters.
  *
  * @param where a where 'clause' in form of (String, String), if the first string ends with '!=' or 'IN' the
  *              operator is used in the query, otherwise a '=' is added in between; AND-ing is assumed
  */
case class BooleanQuery(
                         where: Option[Seq[(String, String)]] = None,
                         queryID: Option[String] = Some(java.util.UUID.randomUUID().toString))
  extends QueryObject(queryID) {
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
        this.where.equals(that.where)
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + where.hashCode()
    result
  }
}


/**
  * Nearest neighbour query parameters.
  *
  * @param attribute     name of attribute to perform query on
  * @param q          query vector
  * @param distance   distance function
  * @param k          number of elements to retrieve
  * @param indexOnly  if set to true, then only the index is scanned and the results are result candidates only
  *                   and may contain false positives
  * @param partitions partitions to query (if not set all partitions are queried)
  * @param options    options to pass to handler
  */
case class NearestNeighbourQuery(
                                  attribute: String,
                                  q: FeatureVector,
                                  weights: Option[FeatureVector],
                                  distance: DistanceFunction,
                                  k: Int,
                                  indexOnly: Boolean = false,
                                  options: Map[String, String] = Map[String, String](),
                                  partitions: Option[Set[PartitionID]] = None,
                                  queryID: Option[String] = Some(java.util.UUID.randomUUID().toString))
  extends QueryObject(queryID) {

  def isConform(entity: Entity): Boolean = {
    if (options.getOrElse("nochecks", "false").equals("true")) {
      true
    } else {
      //check if attribute exists
      val attributeExists = entity.schema(Some(Seq(attribute))).nonEmpty

      //check if feature data exists and dimensionality is correct
      val featureData = if (entity.getFeatureData.isDefined) {
        var ndims = CatalogOperator.getAttributeOption(entity.entityname, attribute, Some("ndims")).get.get("ndims")

        if(ndims.isEmpty){
          ndims = Some(entity.getFeatureData.get.select(attribute).head().getAs[FeatureVectorWrapper](attribute).vector.length.toString)
          CatalogOperator.updateAttributeOption(entity.entityname, attribute, "ndims", ndims.get)
        }

        ndims.get.toInt == q.length
      } else {
        false
      }

      attributeExists && featureData
    }
  }

  def isConform(index: Index): Boolean = {
    if (options.getOrElse("nochecks", "false").equals("true")) {
      true
    } else {
      index.isQueryConform(this)
    }
  }

  override def equals(that: Any): Boolean = {
    that match {
      case that: NearestNeighbourQuery =>
        this.attribute.equals(that.attribute) &&
          this.q.equals(that.q) &&
          this.weights.isDefined == that.weights.isDefined &&
          this.weights.map(w1 => that.weights.exists(w2 => w1.equals(w2))).getOrElse(true)
        this.distance.getClass.equals(that.distance.getClass) &&
          this.k == that.k &&
          this.indexOnly == that.indexOnly &&
          this.partitions.isDefined == that.partitions.isDefined &&
          this.partitions.map(p1 => that.partitions.exists(p2 => p1.equals(p2))).getOrElse(true)
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + attribute.hashCode
    result = prime * result + q.hashCode()
    result = prime * result + weights.map(_.hashCode()).getOrElse(0)
    result = prime * result + distance.getClass.hashCode()
    result = prime * result + k
    result = prime * result + indexOnly.hashCode()
    result = prime * result + partitions.map(_.hashCode()).getOrElse(0)
    result
  }
}


