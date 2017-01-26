package org.vitrivr.adampro.index.structures.ecp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.index._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.DistanceFunction
import org.vitrivr.adampro.query.query.NearestNeighbourQuery


/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
class ECPIndex(override val indexname: IndexName)(@transient override implicit val ac: AdamContext)
  extends Index(indexname)(ac) {

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
  override def scan(data: DataFrame, q: MathVector, distance: DistanceFunction, options: Map[String, String], k: Int): DataFrame = {
    log.debug("scanning eCP index " + indexname)

    //for every leader, check its distance to the query-vector, then sort by distance.
    val centroids = meta.leaders.map(l => {
      (l, meta.distance(q, l.vector))
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
    val results = data.filter(col(AttributeNames.featureIndexColumnName) isin (ids.value : _*))
      .withColumn(AttributeNames.distanceColumnName, distUDF(col(AttributeNames.featureIndexColumnName)).cast(DataTypes.FloatType))

    results
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = true
}