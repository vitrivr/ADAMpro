package org.vitrivr.adampro.data.index.structures.ecp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.data.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.data.index._
import org.vitrivr.adampro.data.index.structures.IndexTypes
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.distance.{Distance, DistanceFunction}
import org.vitrivr.adampro.query.query.RankingQuery


/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
class ECPIndex(override val indexname: IndexName)(@transient override implicit val ac: SharedComponentContext)
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
  override def scan(data: DataFrame, q: MathVector, distance: DistanceFunction, options: Map[String, String], k: Int)(tracker : QueryTracker): DataFrame = {
    log.trace("scanning eCP index")

    val numOfElements = options.getOrElse("timesK", "5").toInt * k

    //for every leader, check its distance to the query-vector, then sort by distance.
    val leaders = meta.leaders.map(l => {
      (l, distance(q, l.vector))
    }).sortBy(_._2)

    //take so many leaders up to the moment where the result-length is over k (therefore + 1)
    val numberOfLeadersToUse = leaders.map(_._1.count).scanLeft(0.toLong)(_ + _).takeWhile(_ < numOfElements).length + 1
    val idsBc = ac.sc.broadcast(leaders.take(numberOfLeadersToUse).map(_._1.id))
    val leadersBc = ac.sc.broadcast(leaders.take(numberOfLeadersToUse).map(x => (x._1.id, x._2)).toMap)
    tracker.addBroadcast(idsBc)
    tracker.addBroadcast(leadersBc)
    log.trace("leaders prepared")

    val distUDF = udf((idx : Int) => {
      leadersBc.value.getOrElse(idx, Distance.maxValue)
    })

    //iterate over all leaders until the result-count is over k
    import org.apache.spark.sql.functions._
    val results = data.filter(col(AttributeNames.featureIndexColumnName) isin (idsBc.value : _*))
      .withColumn(AttributeNames.distanceColumnName, distUDF(col(AttributeNames.featureIndexColumnName)).cast(Distance.SparkDistance))

    results
  }

  override def isQueryConform(nnq: RankingQuery): Boolean = true
}