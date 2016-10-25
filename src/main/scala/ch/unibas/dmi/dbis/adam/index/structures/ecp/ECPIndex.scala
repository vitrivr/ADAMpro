package ch.unibas.dmi.dbis.adam.index.structures.ecp

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes


/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
class ECPIndex(override val indexname: IndexName)(@transient override implicit val ac: AdamContext)
  extends Index(indexname) {

  override val indextypename: IndexTypeName = IndexTypes.ECPINDEX
  override val lossy: Boolean = true
  override val confidence = 0.5.toFloat

  val meta = metadata.get.asInstanceOf[ECPIndexMetaData]

  /**
    *
    * @param data     rdd to scan
    * @param q        query vector
    * @param distance distance funciton
    * @param options  options to be passed to the index reader
    * @param k        number of elements to retrieve (of the k nearest neighbor search), possibly more than k elements are returned
    * @return a set of candidate tuple ids, possibly together with a tentative score (the number of tuples will be greater than k)
    */
  override def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, String], k: Int): DataFrame = {
    log.debug("scanning eCP index " + indexname)

    //for every leader, check its distance to the query-vector, then sort by distance.
    val centroids = meta.leaders.map(l => {
      (l, meta.distance(q, l.feature))
    }).sortBy(_._2)

    //take so many centroids up to the moment where the result-length is over k (therefore + 1)
    val numberOfCentroidsToUse = centroids.map(_._1.count).scanLeft(0.toLong)(_ + _).takeWhile(_ < k).length + 1
    val ids = ac.sc.broadcast(centroids.take(numberOfCentroidsToUse).map(_._1.id))

    log.trace("centroids prepared")

    val distUDF = udf((idx : Int) => {
      centroids(idx)._2
    })

    //iterate over all centroids until the result-count is over k
    import org.apache.spark.sql.functions._
    val results = data.filter(col(FieldNames.featureIndexColumnName) isin (ids.value : _*))
      .withColumn(FieldNames.distanceColumnName, distUDF(col(FieldNames.featureIndexColumnName)).cast(DataTypes.FloatType))

    results
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = true
}