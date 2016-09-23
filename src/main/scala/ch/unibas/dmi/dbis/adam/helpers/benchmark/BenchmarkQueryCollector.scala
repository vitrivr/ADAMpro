package ch.unibas.dmi.dbis.adam.helpers.benchmark

import ch.unibas.dmi.dbis.adam.catalog.LogOperator
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
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
trait BenchmarkQueryCollector extends Logging with Serializable {
  def getQueries: Seq[NearestNeighbourQuery]
}

/**
  *
  * @param entityname
  * @param attribute
  * @param nqueries
  * @param options
  */
class BenchmarkRandomQueryCollector(entityname: EntityName, attribute: String, nqueries: Int, options: Map[String, String])(@transient implicit val ac: AdamContext) extends BenchmarkQueryCollector {
  override def getQueries: Seq[NearestNeighbourQuery] = {
    val entity = Entity.load(entityname).get
    val n = entity.count

    val fraction = Sampling.computeFractionForSampleSize(options.get("nqueries").get.toInt, n, withReplacement = false)
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
  * @param options
  */
class LoggedQueryCollector(entityname: EntityName, options: Map[String, String])(@transient implicit val ac: AdamContext) extends BenchmarkQueryCollector {
  override def getQueries: Seq[NearestNeighbourQuery] = LogOperator.getQueries(entityname).get
}
