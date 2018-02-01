package org.vitrivr.adampro.query.query

import org.vitrivr.adampro.data.datatypes.AttributeTypes.{BIT64VECTORTYPE, BYTESVECTORTYPE, SPARSEVECTORTYPE, VECTORTYPE}
import org.vitrivr.adampro.data.datatypes.vector.{ADAMBit64Vector, ADAMBytesVector, ADAMNumericalVector, ADAMVector}
import org.vitrivr.adampro.shared.catalog.CatalogManager
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.data.entity.Entity
import org.vitrivr.adampro.data.entity.Entity.AttributeName
import org.vitrivr.adampro.distribution.partitioning.Partitioning.PartitionID
import org.vitrivr.adampro.data.index.Index
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.distance.DistanceFunction

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
case class FilteringQuery(
                         where: Seq[Predicate],
                         queryID: Option[String] = Some(java.util.UUID.randomUUID().toString))
  extends QueryObject(queryID) {

  override def equals(that: Any): Boolean = {
    that match {
      case that: FilteringQuery =>
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

case class Predicate(attribute : String, operator : Option[String], values : Seq[Any]){
  /**
    * List of SQL operators to keep in query (otherwise a '=' is added)
    */
  lazy val sqlString = {
    val adjustedValues = values.map(value => {
      if(value.isInstanceOf[String]){
       "'" + value + "'"
      } else {
        value
      }
    })

    if(adjustedValues.length > 1 && operator.getOrElse("=").equals("=")){
      "(" + attribute + " IN " + adjustedValues.mkString("(", ",", ")") + ")"
    } else if (adjustedValues.length > 1 && operator.getOrElse("=").equals("!=")){
      "(" + attribute + " NOT IN " + adjustedValues.mkString("(", ",", ")") + ")"
    } else if(adjustedValues.length == 1){
      "(" + attribute + " " + operator.getOrElse(" = ") + " " + adjustedValues.head + ")"
    } else {
      ""
    }
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
case class RankingQuery(
                                  attribute: AttributeName,
                                  q: ADAMVector[_],
                                  weights: Option[ADAMVector[_]],
                                  distance: DistanceFunction,
                                  k: Int,
                                  indexOnly: Boolean = false,
                                  options: Map[String, String] = Map[String, String](),
                                  partitions: Option[Set[PartitionID]] = None,
                                  queryID: Option[String] = Some(java.util.UUID.randomUUID().toString))
  extends QueryObject(queryID) {

  def isConform(entity: Entity)(implicit ac: SharedComponentContext): Boolean = {
    if (options.getOrElse("nochecks", "false").equals("true")) {
      true
    } else {
      //check if attribute exists
      val attributeExists = entity.schema(Some(Seq(attribute))).nonEmpty


      //check if type is correct
      val attributetype = entity.schema(nameFilter = Some(Seq(attribute)), fullSchema = false).head.attributeType
      val queryFeatureDataType = if (q.isInstanceOf[ADAMNumericalVector]) {
        (attributetype == VECTORTYPE || attributetype == SPARSEVECTORTYPE)
      } else if(q.isInstanceOf[ADAMBit64Vector]) {
        (attributetype == BIT64VECTORTYPE)
      } else if(q.isInstanceOf[ADAMBytesVector]) {
        (attributetype == BYTESVECTORTYPE)
      } else {
        false
      }

        //check if feature data exists and dimensionality is correct
      val featureData = if (entity.getFeatureData.isDefined &&  (attributetype == VECTORTYPE || attributetype == SPARSEVECTORTYPE) ) {
        var ndims = ac.catalogManager.getAttributeOption(entity.entityname, attribute, Some("ndims")).get.get("ndims")

        if(ndims.isEmpty){
          ndims = Some(entity.getFeatureData.get.select(attribute).head().getAs[DenseSparkVector](attribute).length.toString)
          ac.catalogManager.updateAttributeOption(entity.entityname, attribute, "ndims", ndims.get)
        }

        ndims.get.toInt == q.length
      } else {
        true
      }

      attributeExists && queryFeatureDataType && featureData
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
      case that: RankingQuery =>
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


