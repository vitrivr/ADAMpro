package ch.unibas.dmi.dbis.adam.query.scanner

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Scanner for the feature data.
  *
  * Ivan Giangreco
  * August 2015
  */
object FeatureScanner {
  val log = Logger.getLogger(getClass.getName)

  /**
    * Scans the feature data based on a nearest neighbour query.
    *
    * @param entity
    * @param query
    * @param filter if filter is defined, we pre-filter for the features
    * @return
    */
  def apply(entity: Entity, query: NearestNeighbourQuery, filter: Option[Set[TupleID]])(implicit ac: AdamContext): DataFrame = {
    val data = if (filter.isDefined) {
      //scan based on tuples filtered in index
      log.debug("scan features with pre-filter")
      ac.sc.setLocalProperty("spark.scheduler.pool", "feature")
      ac.sc.setJobGroup(query.queryID.getOrElse(""), entity.entityname, true)
      entity.filter(filter.get)
    } else {
      //sequential scan
      log.debug("scan features without pre-filter")
      ac.sc.setLocalProperty("spark.scheduler.pool", "slow")
      ac.sc.setJobGroup(query.queryID.getOrElse(""), entity.entityname, true)
      entity.getFeaturedata
    }

    val q = ac.sc.broadcast(query.q)

    import org.apache.spark.sql.functions._
    val distUDF = udf((c: FeatureVectorWrapper) => {
      query.distance(q.value, c.vector)
    })

    data
      .withColumn(FieldNames.distanceColumnName, distUDF(data(query.column)))
      .select(col(FieldNames.distanceColumnName), col(FieldNames.idColumnName))
      .orderBy(col(FieldNames.distanceColumnName))
      .limit(query.k)
  }
}
