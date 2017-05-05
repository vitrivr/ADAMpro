package org.vitrivr.adampro.index.structures.sh

import org.apache.spark.sql.{DataFrame, Row}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.bitstring.BitString
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.datatypes.vector.MovableFeature
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.index.{Index}
import org.vitrivr.adampro.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.{DistanceFunction, MinkowskiDistance}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
class SHIndex(override val indexname: IndexName)(@transient override implicit val ac: AdamContext)
  extends Index(indexname)(ac) {

  override val indextypename: IndexTypeName = IndexTypes.SHINDEX
  override val lossy: Boolean = true
  override val confidence = 0.5.toFloat

  val meta = metadata.get.asInstanceOf[SHIndexMetaData]

  /**
    *
    * @param data     rdd to scan
    * @param q        query vector
    * @param distance distance funciton
    * @param options  options to be passed to the index reader
    * @param k        number of elements to retrieve (of the k nearest neighbor search), possibly more than k elements are returned
    * @return a set of candidate tuple ids, possibly together with a tentative score (the number of tuples will be greater than k)
    */
  override def scan(data: DataFrame, q: MathVector, distance: DistanceFunction, options: Map[String, String], k: Int)(tracker : OperationTracker): DataFrame = {
    log.debug("scanning SH index " + indexname)

    val numOfQueries = options.getOrElse("numOfQ", "3").toInt

    import MovableFeature.conv_math2mov
    val originalQuery = SHUtils.hashFeature(q, meta)
    //move the query around by the precomuted radius
    //TODO: possibly adjust weight of computed queries vs. true query
    val queriesBc = ac.sc.broadcast(List.fill(numOfQueries)((1.0, SHUtils.hashFeature(q.move(meta.radius), meta))) ::: List((1.0, originalQuery)))
    tracker.addBroadcast(queriesBc)

    import org.apache.spark.sql.functions.udf
    val distUDF = udf((c: Array[Byte]) => {
      var i = 0
      var score = 0
      while (i < queriesBc.value.length) {
        val weight = queriesBc.value(i)._1
        val query = queriesBc.value(i)._2
        score += BitString.fromByteArray(c).hammingDistance(query) //hamming distance
        i += 1
      }

      score
    })

    val res = data
      .withColumn(AttributeNames.distanceColumnName, distUDF(data(AttributeNames.featureIndexColumnName)))
      .orderBy(AttributeNames.distanceColumnName)
      .limit(k)

    //here we possibly loose some results, if distance is same for many elements

    res
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = {
    if (nnq.distance.isInstanceOf[MinkowskiDistance] && nnq.distance.asInstanceOf[MinkowskiDistance].n == 2) {
      return true
    }

    log.error("SH index can only be used with Euclidean distance")
    false
  }
}