package ch.unibas.dmi.dbis.adam.helpers.benchmark

import ch.unibas.dmi.dbis.adam.catalog.LogOperator
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.utils.Logging
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