package org.vitrivr.adampro.query.planner

import org.apache.spark.util.random.Sampling
import org.vitrivr.adampro.data.datatypes.vector.Vector
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.data.entity.Entity
import org.vitrivr.adampro.data.entity.Entity.EntityName
import org.vitrivr.adampro.utils.exception.GeneralAdamException
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.distance.EuclideanDistance
import org.vitrivr.adampro.query.query.RankingQuery
import org.vitrivr.adampro.utils.Logging

import scala.util.Random

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
private[query] trait QueryCollection extends Logging with Serializable {
  val getQueries: Seq[RankingQuery]
}

/**
  * Generates random queries
  *
  * @param entityname
  * @param attribute
  * @param nqueries
  */
private[planner] case class RandomQueryCollection(entityname: EntityName, attribute: String, nqueries: Int)(@transient implicit val ac: SharedComponentContext) extends QueryCollection {
  def this(entityname: EntityName, attribute: String, params: Map[String, String])(implicit ac: SharedComponentContext) {
    this(entityname, attribute, params.get("nqueries").get.toInt)
  }

  lazy val getQueries: Seq[RankingQuery] = {
    val entity = Entity.load(entityname).get
    val n = entity.count


    val fraction = Sampling.computeFractionForSampleSize(nqueries, n, withReplacement = false)
    val queries = entity.getFeatureData.get
      .sample(withReplacement = false, fraction = fraction)
      .collect()
      .map(r => r.getAs[Seq[VectorBase]](attribute))
      .map(vec => RankingQuery(attribute, Vector.conv_draw2vec(vec), None, EuclideanDistance, 100, true))

    queries
  }
}

/**
  * Uses logged queries.
  *
  * @param entityname
  */
private[planner] case class LoggedQueryCollection(entityname: EntityName, attribute: String, nqueries : Option[Int] = None)(@transient implicit val ac: SharedComponentContext) extends QueryCollection {
  def this(entityname: EntityName, attribute: String, params: Map[String, String])(implicit ac: SharedComponentContext) {
    this(entityname, attribute, params.get("nqueries").map(_.toInt))
  }

  lazy val getQueries: Seq[RankingQuery] = {
    var queries = ac.logManager.getQueries(entityname, attribute).get

    //sample
    if(nqueries.isDefined){
      queries = queries.map(x => (Random.nextFloat(), x)).sortBy(_._1).map(_._2).take(nqueries.get)
    }

    //TODO: throw away logs after training has been done on data?

    queries
  }
}


object QueryCollectionFactory {
  def apply(entityname: EntityName, attribute: String, qco: QueryCollectionOption, params: Map[String, String])(implicit ac: SharedComponentContext): QueryCollection = {
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