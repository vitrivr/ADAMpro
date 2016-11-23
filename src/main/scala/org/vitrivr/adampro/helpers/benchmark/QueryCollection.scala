package org.vitrivr.adampro.helpers.benchmark

import org.vitrivr.adampro.catalog.LogOperator
import org.vitrivr.adampro.datatypes.feature.FeatureVectorWrapper
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.EuclideanDistance
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.vitrivr.adampro.utils.Logging
import org.apache.spark.util.random.Sampling

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
private[benchmark] trait QueryCollection extends Logging with Serializable {
  def getQueries: Seq[NearestNeighbourQuery]
}

/**
  * Generates random queries
  *
  * @param entityname
  * @param attribute
  * @param nqueries
  */
private[benchmark] case class RandomQueryCollection(entityname: EntityName, attribute: String, nqueries: Int)(@transient implicit val ac: AdamContext) extends QueryCollection {
  def this(entityname: EntityName, attribute: String, params: Map[String, String])(implicit ac: AdamContext) {
    this(entityname, attribute, params.get("nqueries").get.toInt)
  }

  override def getQueries: Seq[NearestNeighbourQuery] = {
    val entity = Entity.load(entityname).get
    val n = entity.count

    val fraction = Sampling.computeFractionForSampleSize(nqueries, n, withReplacement = false)
    val queries = entity.getFeatureData.get
      .sample(withReplacement = false, fraction = fraction)
      .map(r => r.getAs[FeatureVectorWrapper](attribute))
      .collect()
      .map(vec => NearestNeighbourQuery(attribute, vec.vector, None, EuclideanDistance, 100, true))

    queries
  }
}

/**
  * Uses logged queries.
  *
  * @param entityname
  */
private[benchmark] case class LoggedQueryCollection(entityname: EntityName, attribute: String)(@transient implicit val ac: AdamContext) extends QueryCollection {
  def this(entityname: EntityName, attribute: String, params: Map[String, String])(implicit ac: AdamContext) {
    this(entityname, attribute)
  }

  override def getQueries: Seq[NearestNeighbourQuery] = LogOperator.getQueries(entityname, attribute).get
}


object QueryCollectionFactory {
  def apply(entityname: EntityName, attribute: String, qco: QueryCollectionOption, params: Map[String, String])(implicit ac: AdamContext): QueryCollection = {
    qco match {
      case RandomQueryCollectionOption => new RandomQueryCollection(entityname, attribute, params)
      case LoggedQueryCollectionOption => new LoggedQueryCollection(entityname, attribute, params)
      case _ => throw new GeneralAdamException("query collection option not known")
    }
  }

  sealed abstract class QueryCollectionOption

  case object RandomQueryCollectionOption extends QueryCollectionOption

  case object LoggedQueryCollectionOption extends QueryCollectionOption

}