package ch.unibas.dmi.dbis.adam.index.structures.ecp

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataTypes


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

  /**
    *
    * @param data     rdd to scan
    * @param q        query vector
    * @param distance distance funciton
    * @param options  options to be passed to the index reader
    * @param k        number of elements to retrieve (of the k nearest neighbor search), possibly more than k elements are returned
    * @return a set of candidate tuple ids, possibly together with a tentative score (the number of tuples will be greater than k)
    */
  override def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, Any], k: Int): DataFrame = {
    log.debug("scanning eCP index " + indexname)

    //for every leader, check its distance to the query-vector, then sort by distance.
    val centroids = ac.sc.broadcast(metadata.leaders.map(l => {
      (l.id, metadata.distance(q, l.feature))
    }).sortBy(_._2))

    log.trace("centroids prepared")

    //iterate over all centroids until the result-count is over k
    import org.apache.spark.sql.functions.lit
    var results : DataFrame = null
    var i = 0
    var counter = 0
    do  {
      val nns = data.filter(data(FieldNames.featureIndexColumnName) === centroids.value(i)._1).select(pk.name).withColumn(FieldNames.distanceColumnName, lit(centroids.value(i)._2).cast(DataTypes.FloatType))
      if(results != null) {
        results = results.unionAll(nns)
      } else {
        results = nns
      }
      counter += nns.count().toInt
      i += 1
    } while(i < centroids.value.length && counter < k)

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