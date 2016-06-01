package ch.unibas.dmi.dbis.adam.index.structures.ecp

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.va.VAResultHandler
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
class ECPIndex(val indexname: IndexName, val entityname: EntityName, override private[index] var data: DataFrame, private[index] val metadata: ECPIndexMetaData)(@transient override implicit val ac: AdamContext)
  extends Index {

  override val indextypename: IndexTypeName = IndexTypes.ECPINDEX

  override val lossy: Boolean = true
  override val confidence = 0.toFloat

  override def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, Any], k: Int): DataFrame = {
    log.debug("scanning eCP index " + indexname)

    val centroids = ac.sc.broadcast(metadata.leaders.map(l => {
      (l.id, metadata.distance(q, l.feature))
    }).sortBy(_._2))

    log.trace("centroids prepared")

    import org.apache.spark.sql.functions.lit
    var results : DataFrame = null
    var i = 0
    do  {
      val nns = data.filter(data(FieldNames.featureIndexColumnName) === centroids.value(i)._1).select(pk.name).withColumn(FieldNames.distanceColumnName, lit(centroids.value(i)._2))
      if(results != null) {
        results.unionAll(nns)
      } else {
        results = nns
      }
      i += 1
    } while(i < centroids.value.length && results.count() < k)

    results
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = true
}

object ECPIndex {
  def apply(indexname: IndexName, tablename: EntityName, data: DataFrame, meta: Any)(implicit ac: AdamContext): ECPIndex = {
    val indexMetaData = meta.asInstanceOf[ECPIndexMetaData]
    new ECPIndex(indexname, tablename, data, indexMetaData)
  }
}