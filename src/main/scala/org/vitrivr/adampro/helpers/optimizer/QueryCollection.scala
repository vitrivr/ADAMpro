package org.vitrivr.adampro.helpers.optimizer

import org.apache.spark.util.random.Sampling
import org.vitrivr.adampro.datatypes.vector.Vector
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.exception.GeneralAdamException
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.query.distance.EuclideanDistance
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.vitrivr.adampro.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
private[helpers] trait QueryCollection extends Logging with Serializable {
  def getQueries: Seq[NearestNeighbourQuery]
}

/**
  * Generates random queries
  *
  * @param entityname
  * @param attribute
  * @param nqueries
  */
private[optimizer] case class RandomQueryCollection(entityname: EntityName, attribute: String, nqueries: Int)(@transient implicit val ac: AdamContext) extends QueryCollection {
  def this(entityname: EntityName, attribute: String, params: Map[String, String])(implicit ac: AdamContext) {
    this(entityname, attribute, params.get("nqueries").get.toInt)
  }

  override def getQueries: Seq[NearestNeighbourQuery] = {
    val entity = Entity.load(entityname).get
    val n = entity.count


    val fraction = Sampling.computeFractionForSampleSize(nqueries, n, withReplacement = false)
    val queries = entity.getFeatureData.get
      .sample(withReplacement = false, fraction = fraction)
      .collect()
      .map(r => r.getAs[Seq[VectorBase]](attribute))
      .map(vec => NearestNeighbourQuery(attribute, Vector.conv_draw2vec(vec), None, EuclideanDistance, 100, true))

    queries
  }
}

/**
  * Uses logged queries.
  *
  * @param entityname
  */
private[optimizer] case class LoggedQueryCollection(entityname: EntityName, attribute: String)(@transient implicit val ac: AdamContext) extends QueryCollection {
  def this(entityname: EntityName, attribute: String, params: Map[String, String])(implicit ac: AdamContext) {
    this(entityname, attribute)
  }

  override def getQueries: Seq[NearestNeighbourQuery] = SparkStartup.logOperator.getQueries(entityname, attribute).get //TODO: throw away logs after training has been done on data
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